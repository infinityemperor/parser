import asyncio
import aiohttp
import pandas as pd
import random
import string
import os
import re
from datetime import datetime
import ssl

# Генерация значения guestid
def generate_guestid(length=20):
    characters = string.ascii_letters + string.digits + "-_"
    return ''.join(random.choice(characters) for _ in range(length))

# Генерация случайных utm_ параметров
def generate_utm_params():
    return {
        'utm_source': ''.join(random.choices(string.ascii_lowercase + string.digits, k=10)),
        'utm_medium': ''.join(random.choices(string.ascii_lowercase + string.digits, k=10)),
        'utm_campaign': ''.join(random.choices(string.ascii_lowercase + string.digits, k=10))
    }

# Получаем путь к директории, где находится текущий скрипт
current_dir = os.path.dirname(os.path.abspath(__file__))
cert_file_path = os.path.join(current_dir, 'BrightData.crt')

# Список пресетов User-Agent
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, как Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    # Добавьте больше User-Agent'ов, если необходимо
]

# Функция для получения случайного User-Agent
def get_random_user_agent():
    return random.choice(USER_AGENTS)

# Класс для управления сессией с изменением прокси
class ProxySessionManager:
    def __init__(self, username, password):
        self.username = username
        self.password = password

    def get_proxy(self):
        session_id = str(random.random())
        proxy = f"http://{self.username}-session-{session_id}:{self.password}@brd.superproxy.io:22225"
        return proxy

# Асинхронная функция для выполнения запроса с SSL сертификатом и повторными попытками
async def fetch_data(session, url, proxy_manager, max_retries=20):
    headers = {'User-Agent': get_random_user_agent()}
    cookies = {'guestid': generate_guestid(), **generate_utm_params()}

    for attempt in range(max_retries):
        proxy = proxy_manager.get_proxy()
        try:
            async with session.get(url, headers=headers, cookies=cookies, proxy=proxy, timeout=30) as response:
                if response.status == 200:
                    response_data = await response.json()
                    if response_data.get("code") == "200":
                        return response_data
        except Exception:
            pass

    return None

# Очистка shortName от нежелательных символов
def clean_short_name(short_name):
    return re.sub(r'[^a-zA-Z0-9]', '', short_name)

# Обработка ответа и извлечение данных
def process_response(response_data, brandid):
    if not response_data or 'data' not in response_data:
        return []

    data = []
    for item in response_data.get('data', []):
        if str(item.get('catalogId')) == brandid:
            short_name = clean_short_name(item.get('shortName', ''))
            quantity = item.get('quantity', 0)
            delivery_days = item.get('deliveryDays', float('inf'))
            price_id = item.get('priceId')  # Новый элемент
            if quantity > 2 and delivery_days < 11:
                extracted_data = {
                    'catalogName': item.get('catalogName'),
                    'shortName': short_name,
                    'name': item.get('name'),
                    'price': item.get('price'),
                    'quantity': quantity,
                    'deliveryDays': delivery_days,
                    'priceId': price_id  # Добавляем в таблицу
                }
                data.append(extracted_data)

    return sorted(data, key=lambda x: x['price'])[:10]

# Асинхронная функция для обработки одной строки
async def process_line(session, line, proxy_manager, semaphore, status, fail_log):
    id_, brandid = line.strip().split(':')
    url = f'http://autopiter.ru/api/api/appraise?id={id_}'

    async with semaphore:
        response_data = await fetch_data(session, url, proxy_manager)

        if response_data:
            status["ok"] += 1
            return process_response(response_data, brandid)
        else:
            # Логируем неудачные запросы
            with open(fail_log, 'a') as f:
                f.write(f"{url}\n")
        return []

# Функция для отображения статуса
async def print_status(status):
    while True:
        in_process = status['total'] - status['ok']
        print(f"Total: {status['total']} | In Process: {in_process} | Ok: {status['ok']}")
        await asyncio.sleep(10)  # Обновляем статус каждые 10 секунд

# Основная асинхронная функция
async def main():
    input_file = 'autopiterpars.txt'
    today_date = datetime.now().strftime('%Y%m%d')
    output_file = f'output_autopiter_{today_date}.xlsx'
    fail_log = 'failsAP.txt'

    if os.path.exists(output_file):
        os.remove(output_file)

    with open(input_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    proxy_manager = ProxySessionManager(
        'brd-customer-hl_b6d31457-zone-residential_proxy1',
        'cehcr8vujc5j'
    )

    ssl_context = ssl.create_default_context(cafile=cert_file_path)
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    semaphore = asyncio.Semaphore(10)  # Ограничение на 10 параллельных запросов

    status = {"total": len(lines), "ok": 0}

    asyncio.create_task(print_status(status))

    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context)) as session:
        tasks = [process_line(session, line, proxy_manager, semaphore, status, fail_log) for line in lines]
        results = await asyncio.gather(*tasks)
    
    all_data = [item for sublist in results for item in sublist]

    if all_data:
        df = pd.DataFrame(all_data, columns=['catalogName', 'shortName', 'name', 'price', 'quantity', 'deliveryDays', 'priceId'])
        df.to_excel(output_file, index=False)

if __name__ == "__main__":
    asyncio.run(main())
