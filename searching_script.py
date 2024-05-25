import requests
import time
from bs4 import BeautifulSoup
import os
import threading
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer
import json
import uuid
import logging
import re

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')
load_dotenv()

file_lock = threading.Lock()

VK_ACCESS_TOKEN = os.getenv('VK_ACCESS_TOKEN')

OLYMPICS_PATTERN = re.compile(r'^(.*?),(.*?),(.*?),(.*?),(.*?),\[(.*?)\]$')
CONFERENCE_PATTERN = re.compile(r'^(.*?),(.*?),(.*?),(.*?)$')

def get_kafka_consumer(topic):
    return KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )


def get_kafka_producer():
    return KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda m: json.dumps(m).encode('utf-8')
    )


def get_admin_center_from_wikipedia(region):
    if 'г.' in region:
        return region.replace('г.', '').strip()

    search_url = "https://ru.wikipedia.org/wiki/"
    try:
        response = requests.get(search_url + region)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            infobox = soup.find('table', {'class': 'infobox'})
            if infobox:
                for row in infobox.find_all('tr'):
                    header = row.find('th')
                    if header and 'Административный центр' in header.text:
                        value = row.find('td')
                        if value:
                            return value.text.strip()
    except requests.RequestException as e:
        logging.error(f"Error fetching page: {e}")
    return "Не найдено"


def search_vk(last_name, first_name, city, age_from, age_to, access_token):
    url = 'https://api.vk.com/method/users.search'
    name_query = f"{last_name} {first_name}" if first_name else last_name
    hometown = get_admin_center_from_wikipedia(city)
    params = {
        'q': name_query,
        'hometown': hometown,
        'age_from': age_from,
        'age_to': age_to,
        'access_token': access_token,
        'v': '5.131',
        'count': 1,
        'fields': 'bdate, city, education'
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Error searching VK for {name_query}: {e}")
        return {}


def main():
    consumer = get_kafka_consumer('start_searching_topic')
    producer = get_kafka_producer()
    vk_access_token = VK_ACCESS_TOKEN

    try:
        for message in consumer:
            input_file = message.value.strip('"')
            result_dir = os.path.join(os.getcwd(), 'result')
            os.makedirs(result_dir, exist_ok=True)
            input_file_path = os.path.join(result_dir, input_file)
            unique_filename = os.path.join(result_dir, f'output_{uuid.uuid4()}.txt')

            try:
                with file_lock, open(input_file_path, 'r', encoding='utf-8') as infile, open(unique_filename, 'w', encoding='utf-8') as outfile:
                    header = infile.readline().strip()
                    for line in infile:
                        if line.strip():
                            olympics_match = OLYMPICS_PATTERN.match(line)
                            if olympics_match:
                                name, city, school, grade, score, target = olympics_match.groups()
                                age_from = 16 if grade == "11" else 14
                                age_to = age_from + 2

                                last_name, first_name = name.split()[0], ' '.join(name.split()[1:])
                                vk_results = search_vk(last_name, first_name, city, age_from, age_to, vk_access_token)
                                vk_profiles = vk_results.get('response', {}).get('items', [])
                                profile_links = [f"https://vk.com/id{profile['id']}" for profile in vk_profiles]
                                profiles_str = ', '.join(profile_links) if profile_links else 'No profiles found'

                                outfile.write(
                                    f"Name: {name}, City: {city}, School: {school}, Grade: {grade}, Score: {score}, Subjects: [{target}], VK Profiles: {profiles_str}\n")
                                logging.info(f"Processed: {name}, VK Profiles: {profiles_str}")
                                time.sleep(2)

                            else:
                                conference_match = CONFERENCE_PATTERN.match(line)
                                if conference_match:
                                    name, city, school, grade = conference_match.groups()
                                    age_from = 16 if grade == "11" else 14
                                    age_to = age_from + 2

                                    last_name, first_name = name.split()[0], ' '.join(name.split()[1:])
                                    vk_results = search_vk(last_name, first_name, city, age_from, age_to, vk_access_token)
                                    vk_profiles = vk_results.get('response', {}).get('items', [])
                                    profile_links = [f"https://vk.com/id{profile['id']}" for profile in vk_profiles]
                                    profiles_str = ', '.join(profile_links) if profile_links else 'No profiles found'

                                    outfile.write(f"Name: {name}, City: {city}, School: {school}, Grade: {grade}, VK Profiles: {profiles_str}\n")
                                    logging.info(f"Processed: {name}, VK Profiles: {profiles_str}")
                                    time.sleep(2)

                producer.send('searching_results_topic', {'status': 'completed', 'fileName': unique_filename})
                logging.info(f"Results written to {unique_filename}")

            except FileNotFoundError:
                logging.error(f"Error: File {input_file_path} not found.")
    finally:
        producer.close()
        consumer.close()
        logging.info(f"Kafka producer and consumer closed.")


main()

