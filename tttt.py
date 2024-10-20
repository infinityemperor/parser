import aiohttp
import asyncio
import csv
import random
import ssl
import os
import json
import re
from aiohttp import ClientSession
from socket import gethostbyname
from time import sleep
from datetime import datetime  # Импорт datetime для работы с датой

# SSL сертификат (если все равно используем сертификат, но без верификации)
ca_cert_path = os.path.join(os.getcwd(), 'BrightData.crt')

# Резиденциальные прокси параметры
super_proxy = gethostbyname('brd.superproxy.io')
proxy_username = 'hl_b6d31457'
proxy_password = 'cehcr8vujc5j'
proxy_port = 22225

# Параметры многопоточности и повторных запросов
max_parallel_requests = 30
max_attempts_per_link = 20

# Файл с ссылками
links_file = 'links2.txt'

# CSV файл для сохранения данных
csv_file = f"output_emex_{datetime.now().strftime('%Y%m%d')}.csv"  # Используем datetime

# Файл для неудачных ссылок
failed_links_file = 'failsEM.txt'

# Заголовки для CSV
csv_headers = ['Make', 'Detail Number', 'Name', 'Price', 'Quantity', 'Delivery', 'Warehouse (offerKey)']

# Генерация уникального User-Agent и IP
user_agents = [
    # Desktop
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0',
    # Mobile
    'Mozilla/5.0 (iPhone; CPU iPhone OS 10_3 like Mac OS X) AppleWebKit/602.1.50 (KHTML, like Gecko) CriOS/56.0.2924.75 Mobile/14E5239e Safari/602.1',
    'Mozilla/5.0 (Linux; Android 7.0; Nexus 5X Build/NBD91U) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Mobile Safari/537.36',
    'Mozilla/5.0 (iPad; CPU OS 11_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Mobile/15A5341f Safari/604.1'
]

ip_addresses = [
    '203.0.113.0',  # USA
    '198.51.100.0', # Canada
    '192.0.2.0',    # UK
    '198.51.100.1', # Germany
    '192.0.2.1'     # France
]

# Чистка текста от лишних символов, но с сохранением пробелов для поля detailName
def clean_text(text, keep_spaces=False):
    if keep_spaces:
        # Оставляем пробелы, но убираем все, кроме букв и цифр
        return re.sub(r'[^a-zA-Z0-9 ]', '', text)
    else:
        # Убираем все лишнее, включая пробелы
        return re.sub(r'[^a-zA-Z0-9]', '', text)

# Функция для обработки запроса
async def fetch(session, link, attempts):
    for attempt in range(attempts):
        # Уникальные User-Agent и IP для каждого запроса и каждой попытки
        headers = {
            'User-Agent': random.choice(user_agents)
        }

        # Генерация прокси-ссылки с авторизацией
        proxy = f'http://{proxy_username}:{proxy_password}@{super_proxy}:{proxy_port}'

        try:
            # Делает запрос с указанными прокси и без проверки SSL
            async with session.get(link, headers=headers, proxy=proxy, ssl=False, timeout=20) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    # Проверяем наличие ошибки
                    if not data.get("errorMessage"):
                        # Парсинг данных и возврат успешного результата
                        original = data.get('searchResult', {}).get('originals', [])[0]
                        offers = original.get('offers', [])
                        
                        # Очищаем make и detailNum как обычно
                        cleaned_data = [
                            clean_text(original.get('make', '')),
                            clean_text(original.get('detailNum', '')),
                            # Для detailName сохраняем пробелы
                            clean_text(original.get('name', ''), keep_spaces=True)
                        ]

                        # Возврат до 10 предложений с проверкой условий
                        results = []
                        for offer in offers[:10]:
                            quantity = int(clean_text(str(offer.get('quantity', '')), keep_spaces=False))
                            delivery = float(clean_text(str(offer.get('delivery', {}).get('value', '')), keep_spaces=False))
                            
                            # Проверяем, что quantity > 2 и delivery < 11
                            if quantity > 2 and delivery < 11:
                                results.append(cleaned_data + [
                                    clean_text(str(offer.get('displayPrice', {}).get('value', ''))),
                                    str(quantity),
                                    str(delivery),
                                    clean_text(str(offer.get('offerKey', '')))
                                ])
                        return results
        except Exception:
            pass  # Ошибку пропускаем и продолжаем попытки

    # Если все попытки не удались, записываем ссылку в файл
    with open(failed_links_file, 'a', encoding='utf-8') as fail_file:
        fail_file.write(f"{link}\n")
    return None

# Основная функция для выполнения запросов
async def process_links(links):
    total_links = len(links)
    processed_count = 0

    async with ClientSession() as session:
        tasks = [fetch(session, link.strip(), max_attempts_per_link) for link in links]
        results = await asyncio.gather(*tasks)
        
        # Запись в CSV
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(csv_headers)
            for result in results:
                if result:
                    writer.writerows(result)
                processed_count += 1
                # Вывод статуса
                print(f"Обработано {processed_count} из {total_links} ссылок.")

# Функция чтения файла с ссылками
def read_links(filename):
    with open(filename, 'r', encoding='utf-8', errors='ignore') as file:
        return file.readlines()

# Запуск основного процесса
if __name__ == '__main__':
    links = read_links(links_file)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(process_links(links))
    print(f"Данные успешно сохранены в {csv_file}")
