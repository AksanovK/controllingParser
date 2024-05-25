import os
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from io import BytesIO
from docx import Document
import PyPDF2
from datetime import datetime
import camelot
import threading
import json
from kafka import KafkaProducer, KafkaConsumer
import uuid
import psutil
import logging
from proxy_requests import ProxyRequests

file_lock = threading.Lock()

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')


def log_memory_usage():
    process = psutil.Process(os.getpid())
    logging.info(f"MEMORY: {process.memory_info().rss / 1024 ** 2:.2f}")


def get_kafka_producer():
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda m: json.dumps(m).encode('utf-8')
    )
    return producer


# Закомментируем функцию получения DLQ продюсера
# def get_kafka_dlq_producer():
#     dlq_producer = KafkaProducer(
#         bootstrap_servers='localhost:9092',
#         value_serializer=lambda m: json.dumps(m).encode('utf-8')
#     )
#     return dlq_producer


def get_kafka_consumer(topic):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        value_deserializer=lambda m: m.decode('utf-8')
    )
    return consumer


# def find_files(url, file_type):
#     file_links = []
#     try:
#         response = requests.get(url)
#         response.raise_for_status()
#         soup = BeautifulSoup(response.text, 'html.parser')
#         links = soup.find_all('a', href=True)
#         for link in links:
#             href = link['href']
#             if href.endswith(file_type):
#                 full_url = requests.compat.urljoin(url, href)
#                 file_links.append(full_url)
#     except requests.RequestException as e:
#         print(f"Error accessing {url}: {e}")
#     return file_links


# def extract_text_from_pdf(url):
#     text = ""
#     try:
#         response = requests.get(url)
#         response.raise_for_status()
#         with BytesIO(response.content) as bytes_io:
#             pdf = PyPDF2.PdfReader(bytes_io)
#             for page in pdf.pages:
#                 text += (page.extract_text() or "")
#     except Exception as e:
#         print(f"Error processing PDF {url}: {e}")
#     return text
#
#
# def extract_text_from_docx(url):
#     text = ""
#     try:
#         response = requests.get(url)
#         response.raise_for_status()
#         document = Document(BytesIO(response.content))
#         for paragraph in document.paragraphs:
#             text += paragraph.text + "\n"
#     except Exception as e:
#         print(f"Error processing DOCX {url}: {e}")
#     return text
#
#
# def extract_text_from_excel(url):
#     text = ""
#     try:
#         response = requests.get(url)
#         response.raise_for_status()
#         df = pd.read_excel(BytesIO(response.content))
#         text = df.to_string(index=False)
#     except Exception as e:
#         print(f"Error processing Excel {url}: {e}")
#     return text
#
#
# def extract_text_from_txt(url):
#     try:
#         response = requests.get(url)
#         response.raise_for_status()
#         text = response.text
#     except Exception as e:
#         print(f"Error processing Text file {url}: {e}")
#         text = ""
#     return text


def extract_info(text, event_type=None, subject=None, theme=None):
    olympics_pattern = r"(\w+ \w+)\s+(.+?)\s+(\d+ класс)\s+(\d+)\s+Победитель"
    conferences_pattern = r"(\w+ \w+)\s+(.+?)\s+(\д+ класс)\s+(.+?)\с+Тема"

    results = []

    if event_type == 'olympics':
        matches = re.findall(olympics_pattern, text)
        for match in matches:
            results.append({
                'subject': subject,
                'Name': match[0],
                'School': match[1],
                'Grade': match[2],
                'Score': match[3],
                'Status': 'Winner',
                'Target': subject
            })
    elif event_type == 'scientific':
        matches = re.findall(conferences_pattern, text)
        for match in matches:
            results.append({
                'Name': match[0],
                'School': match[1],
                'Grade': match[2],
                'Theme': match[3],
                'Target': 'conferences'
            })

    return results


def download_pdf(url, download_path):
    try:
        response = requests.get(url)
        response.raise_for_status()
        with open(download_path, 'wb') as file:
            file.write(response.content)
        return True
    except Exception as e:
        print(f"Error downloading PDF {url}: {e}")
        return False


def find_header_index(df):
    for index, row in df.iterrows():
        if 'класс' in ''.join(str(x) for x in row).lower():
            return index
    return None


def extract_results_from_vseros_pdf_camelot(url, subject):
    results = []
    directory = './sources/olympics/vseros/'
    os.makedirs(directory, exist_ok=True)
    logging.error(f"START CAMELOT")
    pdf_path = os.path.join(directory, 'Протокол жюри.pdf')

    if not download_pdf(url, pdf_path):
        return results

    try:
        with open(pdf_path, 'rb') as file:
            reader = PyPDF2.PdfReader(file)
            num_pages = len(reader.pages)

            for i in range(num_pages):
                page = reader.pages[i]
                text = page.extract_text()

                if '11 класс' in text:
                    start_page = i + 1
                    break

        if start_page is not None:
            logging.info(f"Starting table extraction from page {start_page}")
            tables = camelot.read_pdf(pdf_path, pages=f'{start_page}-end', strip_text='\n')

            collecting = False
            for table in tables:
                df = table.df

                if df.empty or df.iloc[0].isnull().all():
                    continue

                headers = df.iloc[0]
                df.columns = headers
                df = df[1:]

                name_col = next((i for i, col in enumerate(headers) if 'Фамилия' in col), 1)
                school_col = next((i for i, col in enumerate(headers) if 'образовательно' in col.lower()), None)
                grade_col = next((i for i, col in enumerate(headers) if 'Класс обучения' in col), None)
                city_col = next((i for i, col in enumerate(headers) if 'Субъект РФ' in col), None)
                score_col = next((i for i, col in enumerate(headers) if 'Результат' in col), None)
                status_col = next((i for i, col in enumerate(headers) if 'Статус' in col), None)

                for index, row in df.iterrows():
                    if grade_col is not None and '11' in str(row.iloc[grade_col]):
                        collecting = True
                    elif grade_col is not None and '11' not in str(row.iloc[grade_col]):
                        collecting = False
                        continue

                    if collecting:
                        results.append({
                            'Name': row.iloc[name_col].strip() if name_col is not None else "",
                            'School': row.iloc[school_col].strip() if school_col is not None else "",
                            'City': row.iloc[city_col].strip() if city_col is not None else "",
                            'Grade': row.iloc[grade_col].strip() if grade_col is not None else "",
                            'Score': row.iloc[score_col].strip() if score_col is not None else "",
                            'Status': row.iloc[status_col].strip() if status_col is not None else "",
                            'Target': subject
                        })

    except Exception as e:
        logging.error(f"Error processing PDF  {pdf_path}: {e}")
    finally:
        if os.path.exists(pdf_path):
            os.remove(pdf_path)

    return results


def gather_vseros_files(subject):
    base_url = "https://olimpiada.ru"
    current_year = datetime.now().year
    year = current_year - 1
    subject_mapping = {
        'physics': 'phys',
        'informatics': 'iikt',
        'mathematics': 'math'
    }

    subject_code = subject_mapping.get(subject)
    if not subject_code:
        return []

    url = f"{base_url}/vos{year}/{subject_code}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        pdf_links = set()

        for link in soup.find_all('a', href=True):
            if 'Протокол жюри' in link.text:
                full_url = requests.compat.urljoin(url, link['href'])
                pdf_links.add(full_url)

        return list(pdf_links)

    except requests.RequestException as e:
        logging.error(f"Error accessing {url}: {e}")
        return []


# def process_sites(event_type, file_type, extractor_function, urls, subject=None):
#     results = []
#     for url in urls:
#         file_links = find_files(url, file_type)
#         for file_link in file_links:
#             text = extractor_function(file_link)
#             results.extend(extract_info(text, event_type=event_type, subject=subject))
#     return results


def gather_urls(directory):
    urls = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file == 'sites.txt':
                with open(os.path.join(root, file), 'r') as f:
                    urls.extend([line.strip() for line in f.readlines()])
    return urls


def write_results_to_file(results):
    logging.info("START WRITING!")
    unique_filename = f"{uuid.uuid4()}_result.txt"
    output_path = os.path.join('./result', unique_filename)

    try:
        with file_lock:
            with open(output_path, 'w', encoding='utf-8') as f:
                for result in results:
                    f.write(
                        f"Name: {result['Name']}, City: {result['City']}, School: {result['School']}, "
                        f"Grade: {result['Grade']}, Score: {result['Score']}, Status: {result['Status']}, "
                        f"Target: {result['Target']}\n"
                    )
        logging.info("WRITED!")
    except Exception as e:
        logging.error(f"Error writing to file {output_path}: {e}")
    finally:
        log_memory_usage()

    return unique_filename


def process_data(event_type, subject, producer):
    log_memory_usage()
    directories = {
        'olympics': {
            'physics': 'sources/olympics/physics',
            'informatics': 'sources/olympics/informatics',
            'mathematics': 'sources/olympics/mathematics',
            'vseros': 'sources/olympics/vseros'
        },
        'scientific': 'sources/conferences'
    }

    # file_mappings = {
    #     '.pdf': extract_text_from_pdf,
    #     '.docx': extract_text_from_docx,
    #     '.xlsx': extract_text_from_excel,
    #     '.txt': extract_text_from_txt
    # }

    all_results = []
    max_users = 16

    # if event_type == 'olympics' and subject:
    #     directory = directories['olympics'][subject]
    #     urls = gather_urls(directory)
    #     for file_type, extractor_function in file_mappings.items():
    #         results = process_sites(event_type, file_type, extractor_function, urls, subject)
    #         all_results.extend(results)
    #         if len(all_results) >= max_users:
    #             all_results = all_results[:max_users]
    #             break
    if len(all_results) < max_users:
        vseros_files = gather_vseros_files(subject)
        log_memory_usage()
        for vseros_file in vseros_files:
            vseros_results = extract_results_from_vseros_pdf_camelot(vseros_file, subject)
            log_memory_usage()
            all_results.extend(vseros_results)
            if len(all_results) >= max_users:
                all_results = all_results[:max_users]
                break
    # elif event_type == 'scientific':
    #     directory = directories['scientific']
    #     urls = gather_urls(directory)
    #     for file_type, extractor_function in file_mappings.items():
    #         results = process_sites(event_type, file_type, extractor_function, urls)
    #         all_results.extend(results)
    #         if len(all_results) >= max_users:
    #             all_results = all_results[:max_users]
    #             break
    log_memory_usage()
    unique_filename = write_results_to_file(all_results)
    producer.send('parsing_results_topic', {
        'status': 'completed',
        'filename': unique_filename
    })
    logging.info(f"Data processing completed and message sent.")


def main():
    consumer = get_kafka_consumer('start_parsing_topic')
    producer = get_kafka_producer()
    # Закомментируем создание DLQ продюсера
    # dlq_producer = get_kafka_dlq_producer()
    try:
        for message in consumer:
            print(f"Received message: {message}")
            if not message.value:
                continue

            try:
                data = json.loads(message.value)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
                continue

            if isinstance(data, str):
                try:
                    data = json.loads(data)
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON a second time: {e}")
                    continue

            if isinstance(data, dict):
                print(f"Received message: {data}")
                event_type = data.get('type')
                subject = data.get('subject')
                print(f"Received start command for event_type: {event_type} and subject: {subject}")
                # Удалим dlq_producer из вызова process_data
                process_data(event_type, subject, producer)
            else:
                print(f"Error: Decoded message is not a dictionary: {data}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        producer.close()
        consumer.close()


main()
