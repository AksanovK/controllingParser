import os
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import threading
from kafka import KafkaConsumer, KafkaProducer
import json
import uuid
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')
file_lock = threading.Lock()
SCHOOLS_DIR = 'sources/schools'
SCHOOLS_FILE = os.path.join(SCHOOLS_DIR, 'sites.txt')

COEFFICIENTS = {
    'winner_olympics': 3.0,
    'participant_olympics': 1.5,
    'conference_theme': 1.5,
    'top_school': 1.0
}

OLYMPICS_PATTERN = re.compile(
    r"Name: (.+?), City: (.+?), School: (.+?), Grade: (\d+), Score: ([\d.,]+), Status: (\w+), Target: (\w+)")
CONFERENCE_PATTERN = re.compile(
    r"Name: (.+?), City: (.+?), School: (.+?), Grade: (\d+), Target: conferences")


def get_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda m: json.dumps(m).encode('utf-8')
    )


def get_kafka_consumer(topic):
    return KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        value_deserializer=lambda m: m.decode('utf-8')
    )


def find_top_schools(base_url):
    current_year = datetime.now().year
    previous_year = current_year - 1
    rankings_page = f"/education/school_regions/Republic_Tatarstan/{previous_year}/"
    try:
        response = requests.get(base_url + rankings_page)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        rows = []
        for row in soup.find_all('tr'):
            columns = row.find_all(['th', 'td'], class_='ranking_content')
            if len(columns) >= 3:
                rank = columns[0].get_text(strip=True)
                school_name = columns[1].get_text(strip=True)
                city = columns[2].get_text(strip=True)
                if city == 'Казань':
                    rows.append((rank, school_name, city))

        top_schools = [row[1] for row in rows[:3]]
        return top_schools
    except requests.RequestException as e:
        logging.error(f"Error accessing {base_url + rankings_page}: {e}")
        return []


def get_schools_url_from_file(schools_file):
    try:
        with open(schools_file, 'r') as f:
            return f.readline().strip()
    except Exception as e:
        logging.error(f"Error reading file {schools_file}: {e}")
        return ""


def evaluate_candidates(file_path, top_schools):
    candidates = {}
    with file_lock:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f.readlines():
                    olympics_match = OLYMPICS_PATTERN.match(line)
                    if olympics_match:
                        name, city, school, grade, score, status, target = olympics_match.groups()
                        if name not in candidates:
                            candidates[name] = {
                                'city': city,
                                'school': school,
                                'grade': grade,
                                'score': 0
                            }
                        if 'olympics' not in candidates[name]:
                            candidates[name]['olympics'] = []
                        candidates[name]['olympics'].append(target)

                        if status == 'победитель':
                            candidates[name]['score'] += COEFFICIENTS['winner_olympics']
                        else:
                            candidates[name]['score'] += COEFFICIENTS['participant_olympics']

                    conference_match = CONFERENCE_PATTERN.match(line)
                    if conference_match:
                        name, city, school, grade = conference_match.groups()
                        if name not in candidates:
                            candidates[name] = {
                                'city': city,
                                'school': school,
                                'grade': grade,
                                'score': 0
                            }
                        if 'conference' not in candidates[name]:
                            candidates[name]['conference'] = []
                        candidates[name]['conference'].append('conferences')
                        candidates[name]['score'] += COEFFICIENTS['conference_theme']

                    if name in candidates and candidates[name]['school'] in top_schools:
                        candidates[name]['score'] += COEFFICIENTS['top_school']
        except IOError as e:
            logging.error(f"Error reading file {file_path}: {e}")
            return pd.DataFrame()

    df = pd.DataFrame.from_dict(candidates, orient='index')
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'name'}, inplace=True)
    if 'score' not in df.columns:
        df['score'] = 0
    df.sort_values(by='score', ascending=False, inplace=True)

    return df


def write_results_to_file(df, file_path):
    with file_lock:
        df.to_csv(file_path, index=False, encoding='utf-8')


def process_results(producer, output_file):
    result_dir = os.path.join(os.getcwd(), 'result')
    os.makedirs(result_dir, exist_ok=True)
    unique_filename = f"{uuid.uuid4()}_ranked_candidates.csv"
    results_file = os.path.join(result_dir, unique_filename)
    base_url = get_schools_url_from_file(SCHOOLS_FILE)
    input_file_path = os.path.join(result_dir, output_file)
    if not base_url:
        logging.error(f"Error: Base URL not found in {SCHOOLS_FILE}")
        return

    top_schools = find_top_schools(base_url)
    if not top_schools:
        logging.error(f"Error: Unable to retrieve top schools")
        return

    df = evaluate_candidates(input_file_path, top_schools)
    write_results_to_file(df, results_file)
    logging.info(f"Results have been written to {results_file}")
    producer.send('ranking_results_topic', {
        'status': 'completed',
        'filename': unique_filename
    })


def main():
    consumer = get_kafka_consumer('start_ranking_topic')
    producer = get_kafka_producer()

    try:
        for message in consumer:
            filename = message.value.strip('"')
            logging.info(f"Received start command with filename: {filename}")
            process_results(producer, filename)
    finally:
        producer.close()
        consumer.close()


main()
