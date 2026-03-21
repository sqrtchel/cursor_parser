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
import logging
from logging.handlers import RotatingFileHandler

#настройка логирования
log_dir = "ParserLogs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_file = os.path.join(log_dir, "parser.log")
handler = RotatingFileHandler(log_file, maxBytes=1*1024*1024, backupCount=3, encoding='utf-8')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[handler, logging.StreamHandler()] # Добавляем вывод в консоль
)


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


    def parse_by_number(self, number, provided_url=None):
        # метод принимает номер закупки и необязательную ссылку на общую информацию

        logging.info(f"Начинаем обработку закупки: {number}")
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

        url_documents = None
        url_info = None
        info_parsed_successfully = False

        # 1. Пробуем использовать предоставленную ссылку (если она есть)
        if provided_url:
            logging.info(f"Обнаружена прямая ссылка, пробуем: {provided_url}")
            url_info = provided_url
            
            # Определяем ФЗ по номеру
            is_223fz = len(str(number)) == 11 and str(number).startswith('3')
            
            # Пытаемся извлечь ID из ссылки
            # Для 223-ФЗ нам нужен только noticeInfoId
            # Для 44-ФЗ может быть как regNumber
            notice_match = re.search(r'noticeInfoId=(\d+)', provided_url)
            reg_match = re.search(r'regNumber=(\d+)', provided_url)

            # Формируем ссылку на документы
            if is_223fz:
                # У 223-ФЗ бывает только noticeInfoId. Если его нет в ссылке, ищем на сайте
                if notice_match:
                    url_documents = f"https://zakupki.gov.ru/epz/order/notice/notice223/documents.html?noticeInfoId={notice_match.group(1)}"
                else:
                    logging.info(f"В предоставленной ссылке для 223-ФЗ нет noticeInfoId. Запускаем поиск по номеру {number}...")
                    notice_id = self._get_223fz_notice_id(number)
                    if notice_id:
                        url_documents = f"https://zakupki.gov.ru/epz/order/notice/notice223/documents.html?noticeInfoId={notice_id}"
            else: # 44-ФЗ
                # У 44-ФЗ бывает только regNumber
                if reg_match:
                    url_documents = f"https://zakupki.gov.ru/epz/order/notice/ea20/view/documents.html?regNumber={reg_match.group(1)}"

            # Пробуем парсить информацию по этой ссылке
            try:
                response_info = requests.get(url_info, headers=headers, timeout=15)
                response_info.raise_for_status()
                soup_info = BeautifulSoup(response_info.text, 'html.parser')
                delivery_date, delivery_text = self._extract_delivery_period_from_info_page(soup_info)
                if delivery_date and delivery_text:
                    self.groups["Период поставки"].append(delivery_text)
                    year_match = re.search(r'\d{4}', delivery_date)
                    self.groups["Год поставки"].append(year_match.group() if year_match else "Год не найден")
                    info_parsed_successfully = True
            except Exception as e:
                logging.error(f"Ошибка при использовании прямой ссылки {url_info}: {e}")

        # 2. Если прямая ссылка не сработала или её не было, запускаем логику по номеру
        if not info_parsed_successfully:
            logging.info("Прямая ссылка не сработала или отсутствует. Запускаем логику по номеру.")
            is_223fz = len(str(number)) == 11 and str(number).startswith('3')

            if is_223fz:
                notice_id = self._get_223fz_notice_id(number)
                if notice_id:
                    url_documents = f"https://zakupki.gov.ru/epz/order/notice/notice223/documents.html?noticeInfoId={notice_id}"
                    url_info = f"https://zakupki.gov.ru/epz/order/notice/notice223/common-info.html?noticeInfoId={notice_id}"
                    logging.info(f"Сформированы ссылки для 223-ФЗ по noticeInfoId: {notice_id}")
                else:
                    logging.warning(f"Не удалось найти noticeInfoId для 223-ФЗ (номер {number}). Парсинг может быть неполным.")
            else: # 44-ФЗ
                url_documents = f"https://zakupki.gov.ru/epz/order/notice/ea20/view/documents.html?regNumber={number}"
                url_info = f"https://zakupki.gov.ru/epz/order/notice/ok20/view/common-info.html?regNumber={number}"

            # Парсим информацию по сформированным ссылкам
            if url_info:
                try:
                    response_info = requests.get(url_info, headers=headers, timeout=15)
                    response_info.raise_for_status()
                    soup_info = BeautifulSoup(response_info.text, 'html.parser')
                    delivery_date, delivery_text = self._extract_delivery_period_from_info_page(soup_info)
                    if delivery_date and delivery_text:
                        self.groups["Период поставки"].append(delivery_text)
                        year_match = re.search(r'\d{4}', delivery_date)
                        self.groups["Год поставки"].append(year_match.group() if year_match else "Год не найден")
                except Exception as e:
                    logging.error(f"Ошибка при парсинге общей информации по номеру: {e}")

        # 3. Парсинг документов (если ссылка на них была сформирована)
        if url_documents:
            try:
                logging.info(f"Загружаем документы по ссылке: {url_documents}")
                response_docs = requests.get(url_documents, headers=headers, timeout=15)
                response_docs.raise_for_status()
                soup_docs = BeautifulSoup(response_docs.text, 'html.parser')
                doc_links = self._find_docx_links(soup_docs, url_documents)
                if doc_links:
                    logging.info(f"Найдено {len(doc_links)} DOCX файлов.")
                    all_paragraphs = []
                    for link in doc_links:
                        paragraphs = self._read_docx_from_url(link)
                        all_paragraphs.extend(paragraphs)
                    self._group_paragraphs(all_paragraphs)
                else:
                    logging.warning(f"DOCX файлы не найдены по ссылке {url_documents}.")
            except Exception as e:
                logging.error(f"Ошибка при загрузке документов: {e}")
        else:
            logging.warning(f"Ссылка на документы не была сформирована для закупки {number}.")

        # Если в итоге ничего не нашли, заполняем заглушками
        if not self.groups["Период поставки"]:
            self.groups["Период поставки"].append("Период поставки не найден")
        if not self.groups["Год поставки"]:
            self.groups["Год поставки"].append("Год не найден")

        self._process_contract_date()
        return self.groups

    def _get_223fz_notice_id(self, number):
        # метод имитирует поиск на сайте закупок для получения noticeInfoId (для 223-ФЗ)

        search_url = f"https://zakupki.gov.ru/epz/order/extendedsearch/results.html?searchString={number}&morphology=on&pageNumber=1&sortDirection=false&recordsPerPage=_10&showLotsInfoHidden=false&sortBy=UPDATE_DATE&fz223=on&af=on&ca=on&pc=on&pa=on"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

        max_repeats = 3
        for attempt in range(max_repeats):
            try:
                logging.info(f"Попытка {attempt + 1} поиска noticeInfoId для 223-ФЗ (номер {number})...")
                response = requests.get(search_url, headers=headers, timeout=15)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')

                results = soup.find_all('div', class_='search-registry-entry-block')
                if not results:
                    # попробуем другой класс, если сайт обновился
                    results = soup.find_all('div', class_='registry-entry__form')

                for res in results:
                    # ищем ссылку с классом m-0 или просто ссылку в заголовке
                    link_node = res.find('a', class_='m-0') or res.find('a', target='_blank')
                    if link_node and link_node.get('href'):
                        href = link_node['href']
                        # вытаскиваем noticeInfoId из href
                        match = re.search(r'noticeInfoId=(\d+)', href)
                        if match:
                            return match.group(1)

                logging.warning(f"Результаты поиска пусты для номера {number} (попытка {attempt + 1})")
            except Exception as e:
                logging.error(f"Ошибка при поиске noticeInfoId (попытка {attempt + 1}): {e}")

        return None


    def _find_docx_links(self, soup, base_url):
        doc_links = set()

        # Ищем все ссылки на странице
        for link in soup.find_all('a'):
            href = link.get('href', '')
            title = link.get('title', '')
            text = link.get_text()

            # Проверяем наличие .docx в ссылке, заголовке или тексте
            is_docx = (
                '.docx' in href.lower() or 
                '.docx' in title.lower() or 
                '.docx' in text.lower()
            )

            if is_docx and href:
                # Игнорируем ссылки на внешние ресурсы, если они не ведут на скачивание
                full_url = urljoin(base_url, href)
                doc_links.add(full_url)
                logging.debug(f"Найдена ссылка на DOCX: {full_url}")

        if not doc_links:
            # Попробуем поискать по кнопкам или иконкам скачивания, если стандартные ссылки не сработали
            for download_icon in soup.find_all(class_=re.compile(r'download', re.I)):
                parent_link = download_icon.find_parent('a')
                if parent_link and parent_link.get('href'):
                    href = parent_link['href']
                    if 'downloaddoc' in href.lower():
                        full_url = urljoin(base_url, href)
                        doc_links.add(full_url)

        logging.info(f"Итого найдено потенциальных ссылок на документы: {len(doc_links)}")
        return list(doc_links)


    def _read_docx_from_url(self, url):
        # метод скачивает docx файл по URL и читает его текст
        paragraphs = []
        try:
            logging.info(f"Загрузка документа: {url}")
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            response = requests.get(url, headers=headers, stream=True, timeout=20)
            response.raise_for_status()

            # Проверяем, что скачанный файл похож на docx
            if response.content[:2] != b'PK':
                logging.warning(f"Файл по ссылке {url} не является валидным DOCX (отсутствует заголовок ZIP).")
                return []

            with NamedTemporaryFile(delete=False, suffix='.docx') as tmp_file:
                tmp_file.write(response.content)
                tmp_path = tmp_file.name

            doc = docx.Document(tmp_path)
            for paragraph in doc.paragraphs:
                if paragraph.text.strip():
                    paragraphs.append(paragraph.text.strip())

            os.unlink(tmp_path)
            logging.info(f"Успешно прочитано {len(paragraphs)} абзацев из {url}")

        except Exception as e:
            logging.error(f"Ошибка при чтении файла {url}: {e}")

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
        logging.warning("Ни в одном вхождении 'Срок исполнения контракта' дата не найдена, ищем любую дату на странице...")

        all_dates = re.findall(r'\d{2}\.\d{2}\.\d{4}', full_text)
        if all_dates:
            logging.info(f"Найдена дата в другом месте: {all_dates[0]}")
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
    """Чтение данных из файла. Поддерживает форматы:
    1. Просто номер закупки
    2. Номер закупки и ссылка через пробел/табуляцию
    """
    if not os.path.exists(file_path):
        logging.error(f"Файл {file_path} не найден.")
        return []
    
    results = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            
            parts = line.split()
            number = parts[0]
            url = parts[1] if len(parts) > 1 else None
            results.append({'number': number, 'url': url})
            
    logging.info(f"Из файла {file_path} загружено {len(results)} записей.")
    return results


def get_db_connection():
    #Создает подключение к БД, используя параметры из .env
    try:
        conn_str = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            f"SERVER={os.getenv('DB_SERVER')};"
            f"DATABASE={os.getenv('DB_DATABASE')};"
            f"UID={os.getenv('DB_USERNAME')};"
            f"PWD={os.getenv('DB_PASSWORD')};"
            "TrustServerCertificate=yes;"
        )
        return pyodbc.connect(conn_str)
    except Exception as e:
        logging.error(f"Ошибка при подключении к БД: {e}")
        return None


def get_numbers_from_db():
    #Получение номеров и ссылок закупок из БД
    logging.info("Попытка получения данных из БД...")
    
    conn = get_db_connection()
    if not conn:
        logging.error("Подключение к БД не удалось, работа в режиме БД невозможна.")
        return []
    
    try:
        cursor = conn.cursor()

        # Фильтруем те закупки, где l.PlanTVal еще не заполнен (IS NULL)
        query = """SELECT DISTINCT t.NotifNr, t.SrcInf
                    FROM [Cursor].[dbo].[Tender] t
                    INNER JOIN [Cursor].[dbo].[Lot] l (nolock) on l.Tender_id = t.tender_id
                    INNER JOIN [Cursor].[dbo].[LotSpec] ls (nolock) on ls.lot_id = l.lot_id
                    WHERE ((FZ_ID = 44 AND NotifNr NOT LIKE '[a-zA-Z]%' AND len(NotifNr) = 19) 
                    OR (len(NotifNr) = 11 and NotifNr like '3%')) 
                    AND t.SYSDATE >= DATEADD(minute, -90, GETDATE()) 
                    AND l.PlanTVal IS NULL"""
        cursor.execute(query)
        results = [{'number': row[0], 'url': row[1]} for row in cursor.fetchall()]
        conn.close()
        logging.info(f"Из БД загружено {len(results)} записей для обработки.")
        return results
    except Exception as e:
        logging.error(f"Ошибка при выполнении запроса к БД: {e}")
        return []


def save_to_db(number, data):
    #Сохранение результатов в БД
    logging.info(f"Сохранение результатов для закупки {number} в БД...")
    
    conn = get_db_connection()
    if not conn:
        logging.error(f"Подключение к БД не удалось. Данные для {number} не сохранены.")
        return
    
    try:
        cursor = conn.cursor()
        
        # Подготавливаем текстовые данные из найденных групп (объединяем абзацы)
        fs = "\n".join(data.get("Источник финансирования", []))
        sl = "\n".join(data.get("Требования к сроку годности", []))
        pt = "\n".join(data.get("Порядок оплаты товаров", []))
        dt = "\n".join(data.get("Условия поставки", []))
        cdn = "\n".join(data.get("Примечание к сроку действия контракта", []))
        cd = "\n".join(data.get("Срок действия контракта", []))
        tp = "\n".join(data.get("Период поставки", []))
        ty = "\n".join(data.get("Год поставки", []))

        # 1. Обновляем таблицу [Tender]
        query_tender = """
        UPDATE [Cursor].[dbo].[Tender]
        SET TenderDocReglament = ?, RequirementToExpiryDate = ?
        WHERE NotifNr = ?
        """
        cursor.execute(query_tender, (fs, sl, number))
        
        # 2. Обновляем таблицу [Lot]
        # Обновляем все лоты, связанные с этим номером закупки (через Tender_id)
        query_lot = """
        UPDATE l
        SET l.PaymentReglament = ?, 
            l.PlanTVal = ?, 
            l.ContrExpVal = ?, 
            l.SupplyDt = ?,
            l.PlanTPeriod = ?,
            l.PlanTYear = ?
        FROM [Cursor].[dbo].[Lot] l
        INNER JOIN [Cursor].[dbo].[Tender] t ON l.Tender_id = t.tender_id
        WHERE t.NotifNr = ?
        """
        cursor.execute(query_lot, (pt, dt, cdn, cd, tp, ty, number))
        
        conn.commit()
        conn.close()
        logging.info(f"Данные для {number} успешно сохранены в таблицы Tender и Lot.")
    except Exception as e:
        logging.error(f"Ошибка при сохранении в БД для {number}: {e}")


def main():
    logging.info("Запуск парсера.")
    args = get_args()
    parser = DocumentParser()
    
    numbers = []
    
    if args.mode == 'file':
        if not args.input:
            logging.error("Для режима 'file' необходимо указать путь к файлу через --input")
            return
        numbers = read_numbers_from_file(args.input)
    elif args.mode == 'db':
        numbers = get_numbers_from_db()

    if not numbers:
        logging.warning("Список номеров для обработки пуст. Завершение работы.")
        return

    logging.info(f"Всего предстоит обработать закупок: {len(numbers)}")

    for entry in numbers:
        number = entry['number']
        url = entry['url']
        try:
            # Сбрасываем группы перед каждым новым парсингом
            parser.groups = {group: [] for group in parser.groups_config.keys()}
            parser.contract_date = None
            
            # Передаем и номер, и ссылку в метод парсинга
            groups = parser.parse_by_number(number, provided_url=url)
            
            # Сохранение в БД (временная заглушка)
            # save_to_db(number, groups)
            
            logging.info(f"Закупка {number} успешно обработана.")

        except Exception as e:
            logging.error(f"Критическая ошибка при обработке закупки {number}: {e}")

    logging.info("Работа парсера завершена.")

if __name__ == "__main__":
    main()
