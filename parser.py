import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import docx
from tempfile import NamedTemporaryFile
import re


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



def main():

    parser = DocumentParser()

    number = input("Введите номер закупки: ").strip()

    try:

        groups = parser.parse_by_number(number)


        for group_name, paragraphs in groups.items():
            print(f"\n{group_name}")
            print(f"   Количество абзацев: {len(paragraphs)}")
            print("-" * 50)

            if paragraphs:
                for i, p in enumerate(paragraphs, 1):
                    # сокращаем длинные абзацы для вывода
                    if len(p) > 150:
                        p = p[:150] + "..."
                    print(f"{i}. {p}")
            else:
                print("   (нет абзацев в этой группе)")

    except Exception as e:
        print(f"Ошибка: {e}")



if __name__ == "__main__":
     main()
