import sys
import argparse
import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import docx
from tempfile import NamedTemporaryFile
import re
import pyodbc
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()


class DocumentParser:
    def __init__(self):
        self.groups_config = {
            "Источник финансирования": ["финансирован", "за счет", "источник"],
            "Требования к сроку годности": ["годн", "остаточн"],
            "Порядок оплаты товаров": ["оплат"],
            "Условия поставки": ["порядок постав"],
            "Примечание к сроку действия контракта": ["срок исполн"],
            "Срок действия контракта": [],
            "Период поставки": [],
            "Год поставки": []
        }

        self.groups = {group: [] for group in self.groups_config.keys()}

        # для хранения найденной даты из группы "Примечание к сроку действия контракта"
        self.contract_date = None


    def parse_by_number(self, number):
        # метод принимает номер закупки, формирует URL и парсит данные с двух страниц

        print(f"\nНачинаем парсинг закупки с номером: {number}")

        url_documents = f"https://zakupki.gov.ru/epz/order/notice/ea20/view/documents.html?regNumber={number}"
        url_info = f"https://zakupki.gov.ru/epz/order/notice/ok20/view/common-info.html?regNumber={number}"

        #шаг1. ищем и качаем DOCX
        print(f"\nШаг 1. Обрабатываем страницу документов.")
        print(f"Переходим по ссылке: {url_documents}")

        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            response_docs = requests.get(url_documents, headers=headers, timeout=15)
            response_docs.raise_for_status()
            soup_docs = BeautifulSoup(response_docs.text, 'html.parser')

            # ищем и скачиваем DOCX файлы
            doc_links = self._find_docx_links(soup_docs, url_documents)
            if doc_links:
                print(f"Найдено {len(doc_links)} DOCX файлов.")
                all_paragraphs = []
                for link in doc_links:
                    paragraphs = self._read_docx_from_url(link)
                    all_paragraphs.extend(paragraphs)
                # группируем текст из документов
                self._group_paragraphs(all_paragraphs)
            else:
                print("DOCX файлы не найдены на странице.")

        except Exception as e:
            print(f"Ошибка при обработке страницы документов: {e}")

        # шаг2. парсим страницу с общей информацией (ищем дату поставки)
        print(f"\nШаг 2. Обрабатываем страницу с общей информацией.")
        print(f"Переходим по ссылке: {url_info}")

        try:
            response_info = requests.get(url_info, headers=headers, timeout=15)
            response_info.raise_for_status()
            soup_info = BeautifulSoup(response_info.text, 'html.parser')

            delivery_date, delivery_text = self._extract_delivery_period_from_info_page(soup_info)

            #заполняем группы на основе полученных значений
            if delivery_date and delivery_text:
                self.groups["Период поставки"].append(delivery_text)

                # Извлекаем год из даты
                year_match = re.search(r'\d{4}', delivery_date)
                if year_match:
                    year = year_match.group()
                    self.groups["Год поставки"].append(year)
                else:
                    self.groups["Год поставки"].append("Год не найден")
                    print(f"   Год не извлечен из даты {delivery_date}")
            else:
                print("   Период поставки не найден на странице")
                #добавляем заглушки, если группы еще пусты
                if not self.groups["Период поставки"]:
                    self.groups["Период поставки"].append("Период поставки не найден")
                if not self.groups["Год поставки"]:
                    self.groups["Год поставки"].append("Год не найден")

        except Exception as e:
            print(f"Ошибка при обработке страницы с информацией: {e}")
            # если страница не открылась, добавляем заглушки в новые группы
            if not self.groups["Период поставки"]:
                self.groups["Период поставки"].append("Период поставки не найден (ошибка загрузки)")
            if not self.groups["Год поставки"]:
                self.groups["Год поставки"].append("Год не найден (ошибка загрузки)")

        self._process_contract_date()

        return self.groups


    def _find_docx_links(self, soup, base_url):
        #метод ищет документы в атрибутах title и других местах

        doc_links = set()

        for link in soup.find_all('a', title=True):
            title = link['title']
            if '.docx' in title.lower():
                href = link.get('href', '')
                if href:
                    full_url = urljoin(base_url, href)
                    doc_links.add(full_url)
                else:
                    print(f"Найдено название документа: {title}, но нет ссылки")

        return list(doc_links)


    def _read_docx_from_url(self, url):
        # метод скачивает docx файл по URL и читает его текст

        paragraphs = []

        try:
            #скачиваем файл
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(url, headers=headers, stream=True)
            response.raise_for_status()

            #создаем временный файл
            with NamedTemporaryFile(delete=False, suffix='.docx') as tmp_file:
                tmp_file.write(response.content)
                tmp_path = tmp_file.name

            #читаем документ
            doc = docx.Document(tmp_path)

            #извлекаем текст из всех параграфов
            for paragraph in doc.paragraphs:
                if paragraph.text.strip():  #пропускаем пустые параграфы
                    paragraphs.append(paragraph.text.strip())

            #удаляем временный файл
            os.unlink(tmp_path)

        except Exception as e:
            print(f"Ошибка при чтении файла {url}: {e}")

        return paragraphs

    def _check_keywords_in_text(self, text, keywords):
        #метод проверяет, содержит ли текст хотя бы одно из ключевых слов

        text_lower = text.lower()
        for keyword in keywords:
            if keyword.lower() in text_lower:
                return True
        return False

    def _extract_date(self, text):
        #метод извлекает дату из текста

        date_pattern = r'\d{2}\.\d{2}\.\d{2,4}'
        dates = re.findall(date_pattern, text)

        if dates:
            return dates[0]  #берем первую найденную дату
        return None

    def _extract_delivery_period_from_info_page(self, soup):
        # метод ищет все вхождения "срок исполнения контракта" и проверяет каждое, пока не найдет дату

        #получаем весь текст страницы
        full_text = soup.get_text()

        # находим все позиции фразы "Срок исполнения контракта"
        search_text = "Срок исполнения контракта"
        positions = []
        start = 0

        while True:
            pos = full_text.find(search_text, start)
            if pos == -1:
                break
            positions.append(pos)
            start = pos + len(search_text)


        #проверяем каждую позицию по порядку
        for idx, pos in enumerate(positions):

            # берем текст после этой позиции (следующие 200 символов)
            after_text = full_text[pos + len(search_text):pos + len(search_text) + 200]

            # ищем дату
            date_match = re.search(r'(\d{2}\.\d{2}\.\d{4})', after_text)

            if date_match:
                found_date = date_match.group(1)

                #пытаемся найти полный текст периода (может быть "31.12.2026" или "до 31.12.2026")
                period_text = found_date
                #проверяем, есть ли перед датой слово "до" или другой текст
                before_date = after_text[:date_match.start()].strip()
                if before_date and len(before_date) < 20:  #короткий текст перед датой
                    period_text = f"{before_date} {found_date}"

                return found_date, period_text

        # если ни одно вхождение не дало даты, пробуем найти любую дату на странице
        print("    Ни в одном вхождении дата не найдена")
        print("    Ищем любую дату на странице...")

        all_dates = re.findall(r'\d{2}\.\d{2}\.\d{4}', full_text)
        if all_dates:
            print(f"    Найдена дата в другом месте: {all_dates[0]}")
            return all_dates[0], all_dates[0]

        return None, None


    def _group_paragraphs(self, paragraphs):
        #метод группирует параграфы по ключевым словам

        for paragraph in paragraphs:
            # проверяем каждую группу (кроме специальной группы с датой)
            for group_name, keywords in self.groups_config.items():
                if group_name == "Срок действия контракта":
                    continue

                if self._check_keywords_in_text(paragraph, keywords):
                    self.groups[group_name].append(paragraph)

                    #если это группа "Примечание к сроку действия контракта", ищем дату
                    if group_name == "Примечание к сроку действия контракта":
                        date = self._extract_date(paragraph)
                        if date:
                            self.contract_date = date

    def _process_contract_date(self):
        #метод обрабатывает специальную группу "Срок действия контракта"

        if self.contract_date:
            self.groups["Срок действия контракта"] = [f"Дата исполнения контракта: {self.contract_date}"]
        else:
            self.groups["Срок действия контракта"] = ["Дата не найдена"]



def get_args():
    #Настройка аргументов командной строки
    ap = argparse.ArgumentParser(description="Парсер для закупок")
    ap.add_argument('--mode', required=True, choices=['file', 'db'],
                    help='Режим ввода данных. Из файла или базы данных.')
    ap.add_argument('--input', type=str, help='Путь к файлу с номерами закупок (для режима file)')
    return ap.parse_args()


def read_numbers_from_file(file_path):
    #Чтение номеров закупок из файла
    if not os.path.exists(file_path):
        print(f"Ошибка: Файл {file_path} не найден.")
        return []
    
    with open(file_path, 'r', encoding='utf-8') as f:
        # Читаем строки, удаляем пробелы и пустые строки
        numbers = [line.strip() for line in f if line.strip()]
    return numbers


def get_db_connection():
    #Создает подключение к БД, используя параметры из .env
    try:
        conn_str = (
            f"DRIVER={os.getenv('DB_DRIVER', '{SQL Server}')};"
            f"SERVER={os.getenv('DB_SERVER')};"
            f"DATABASE={os.getenv('DB_DATABASE')};"
            f"UID={os.getenv('DB_USERNAME')};"
            f"PWD={os.getenv('DB_PASSWORD')};"
        )
        return pyodbc.connect(conn_str)
    except Exception as e:
        print(f"Ошибка при подключении к БД: {e}")
        return None


def get_numbers_from_db():
    #Получение номеров закупок из БД (таблица ProcurementInput)
    print(">>> Попытка получения данных из БД...")
    
    conn = get_db_connection()
    if not conn:
        print(">>> Подключение к БД не удалось, возвращаем тестовый список.")
        return ["32413348100", "0373200041524000850"]
    
    try:
        cursor = conn.cursor()
        # Предполагаем, что есть таблица ProcurementInput с колонкой RegNumber
        query = "SELECT RegNumber FROM ProcurementInput WHERE Processed = 0"
        cursor.execute(query)
        numbers = [row[0] for row in cursor.fetchall()]
        conn.close()
        return numbers
    except Exception as e:
        print(f"Ошибка при выполнении запроса к БД: {e}")
        return ["32413348100", "0373200041524000850"]


def save_to_db(number, data):
    #Сохранение результатов в БД (таблица ProcurementResults)
    print(f">>> Сохранение результатов для закупки {number} в БД...")
    
    conn = get_db_connection()
    if not conn:
        print(f">>> Подключение к БД не удалось. Данные для {number} не сохранены.")
        return
    
    try:
        cursor = conn.cursor()
        
        # Пример INSERT запроса
        query = """
        INSERT INTO ProcurementResults (
            RegNumber, FinanceSource, ShelfLife, PaymentTerms, DeliveryTerms, 
            ContractDateNote, ContractDate, DeliveryPeriod, DeliveryYear
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        # Извлекаем данные из словаря data (parser.groups)
        # Группы могут содержать списки абзацев, объединяем их в один текст
        fs = "\n".join(data.get("Источник финансирования", []))
        sl = "\n".join(data.get("Требования к сроку годности", []))
        pt = "\n".join(data.get("Порядок оплаты товаров", []))
        dt = "\n".join(data.get("Условия поставки", []))
        cdn = "\n".join(data.get("Примечание к сроку действия контракта", []))
        cd = "\n".join(data.get("Срок действия контракта", []))
        dp = "\n".join(data.get("Период поставки", []))
        dy = "\n".join(data.get("Год поставки", []))

        cursor.execute(query, (number, fs, sl, pt, dt, cdn, cd, dp, dy))
        
        # Помечаем как обработанное в исходной таблице
        cursor.execute("UPDATE ProcurementInput SET Processed = 1 WHERE RegNumber = ?", (number,))
        
        conn.commit()
        conn.close()
        print(f">>> Данные для {number} успешно сохранены в БД.")
    except Exception as e:
        print(f"Ошибка при сохранении в БД: {e}")


def main():
    args = get_args()
    parser = DocumentParser()
    
    numbers = []
    
    if args.mode == 'file':
        if not args.input:
            print("Ошибка: Для режима 'file' необходимо указать путь к файлу через --input")
            return
        numbers = read_numbers_from_file(args.input)
    elif args.mode == 'db':
        numbers = get_numbers_from_db()

    if not numbers:
        print("Список номеров для обработки пуст.")
        return

    print(f"Найдено номеров для обработки: {len(numbers)}")

    for number in numbers:
        try:
            # Сбрасываем группы перед каждым новым парсингом
            parser.groups = {group: [] for group in parser.groups_config.keys()}
            parser.contract_date = None
            
            groups = parser.parse_by_number(number)

            # Вывод в консоль 
            for group_name, paragraphs in groups.items():
                print(f"\n{group_name}")
                print(f"   Количество абзацев: {len(paragraphs)}")
                print("-" * 50)

                if paragraphs:
                    for i, p in enumerate(paragraphs, 1):
                        if len(p) > 150:
                            p = p[:150] + "..."
                        print(f"{i}. {p}")
                else:
                    print("   (нет абзацев в этой группе)")
            
            # Сохранение в БД (заглушка)
            save_to_db(number, groups)

        except Exception as e:
            print(f"Ошибка при обработке закупки {number}: {e}")


if __name__ == "__main__":
    main()
