from datetime import datetime, timedelta
import os
import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException, ElementNotInteractableException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service


BASE_DIR = "/opt/airflow/parsed_data"
os.makedirs(BASE_DIR, exist_ok=True)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "parse_all_healthcare_data_dag",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
)


def setup_driver() -> webdriver.Chrome:
    opts = Options()
    opts.add_argument("--headless=new")          
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage") 
    return webdriver.Chrome(options=opts)


def extract_multiline_data(cell_html):
    parts = cell_html.replace('<hr style="margin: 7px">', ",").split(",")
    return ", ".join(p.strip() for p in parts if p.strip())

def parse_healthcare_data(**kwargs):
    driver = setup_driver()
    all_data = []
    try:
        driver.get("https://fms.ecc.kz/ru/fsms/healthcare_subjects")
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.table-bordered tbody tr")))
        visited = set()
        while True:
            url = driver.current_url
            if url in visited:
                break
            visited.add(url)
            rows = driver.find_elements(By.CSS_SELECTOR, "table.table-bordered tbody tr")
            for row in rows:
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) >= 16:
                    all_data.append([ 
                        cols[0].text.strip(),
                        cols[1].text.strip(),
                        cols[2].text.strip(),
                        cols[3].text.strip(),
                        extract_multiline_data(cols[4].get_attribute("innerHTML")),
                        cols[5].text.strip(),
                        cols[6].text.strip(),
                        cols[7].text.strip(),
                        cols[8].text.strip(),
                        cols[9].text.strip(),
                        extract_multiline_data(cols[10].get_attribute("innerHTML")),
                        extract_multiline_data(cols[11].get_attribute("innerHTML")),
                        cols[12].text.strip(),
                        cols[13].text.strip(),
                        cols[14].text.strip(),
                        cols[15].text.strip(),
                    ])
            try:
                pagination = driver.find_element(By.CSS_SELECTOR, "ul.pagination")
                arrow = pagination.find_element(By.XPATH, ".//a[text()='>']")
                arrow.click()
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.table-bordered tbody tr")))
            except (NoSuchElementException, ElementNotInteractableException):
                break
    finally:
        driver.quit()
    
    df = pd.DataFrame(all_data, columns=[
        "№", "БИН/ИИН", "Наименование субъекта здравоохранения", "На заключение долгосрочного договора",
        "Юридический адрес", "Форма собственности субъекта здравоохранения",
        "Регион, населению которого будут оказываться услуги", "Дата включения в базу данных",
        "Дата исключения из базы данных", "Дата последнего изменения",
        "Вид/Форма медицинской помощи (Поставщик)", "Подвида вида/формы медицинской помощи (Поставщик)",
        "Вид/Форма медицинской помощи (Соисполнитель)", "Подвида вида/формы медицинской помощи (Соисполнитель)",
        "Статус субъекта здравоохранения", "Статус",
    ])
    ds = kwargs["ds"]
    csv_path = os.path.join(BASE_DIR, f"healthcare_subjects_{ds}.csv")
    xlsx_path = os.path.join(BASE_DIR, f"healthcare_subjects_{ds}.xlsx")
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    df.to_excel(xlsx_path, index=False)
    Variable.set("last_healthcare_csv", csv_path)
    return csv_path

def parse_participants_data(**kwargs):
    driver = setup_driver()
    all_data = []
    try:
        driver.get("https://fms.ecc.kz/ru/register/supplierreg?name_bin_iin_rnn=&country=&region_supplier=")
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.table-bordered tbody tr")))
        visited = set()
        while True:
            url = driver.current_url
            if url in visited:
                break
            visited.add(url)
            rows = driver.find_elements(By.CSS_SELECTOR, "table.table-bordered tbody tr")
            for row in rows:
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) >= 5:
                    all_data.append([ 
                        cols[0].text.strip(),
                        cols[1].text.strip(),
                        cols[2].text.strip(),
                        cols[3].text.strip(),
                        cols[4].text.strip(),
                    ])
            try:
                pagination = driver.find_element(By.CSS_SELECTOR, "ul.pagination")
                arrow = pagination.find_element(By.XPATH, ".//a[text()='>']")
                arrow.click()
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.table-bordered tbody tr")))
            except (NoSuchElementException, ElementNotInteractableException):
                break
    finally:
        driver.quit()
    
    df = pd.DataFrame(all_data, columns=[
        "№ Участника", "Наименование участника", "БИН (ИНН, УНП)", "ИИН", "РНН"
    ])
    ds = kwargs["ds"]
    csv_path = os.path.join(BASE_DIR, f"participants_{ds}.csv")
    xlsx_path = os.path.join(BASE_DIR, f"participants_{ds}.xlsx")
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    df.to_excel(xlsx_path, index=False)
    Variable.set("last_participants_csv", csv_path)
    return csv_path

def parse_announcements_data(**kwargs):
    driver = setup_driver()
    all_data = []
    try:
        driver.get("https://fms.ecc.kz/ru/searchanno")
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.table-bordered tbody tr")))
        while True:
            rows = driver.find_elements(By.CSS_SELECTOR, "table.table-bordered tbody tr")
            for row in rows:
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) >= 10:
                    all_data.append([ 
                        cols[0].text.strip(),
                        cols[1].text.strip(),
                        cols[2].text.strip(),
                        cols[3].text.strip(),
                        cols[4].text.strip(),
                        cols[5].text.strip(),
                        cols[6].text.strip(),
                        cols[7].text.strip(),
                        cols[8].text.strip(),
                        cols[9].text.strip(),
                    ])
            try:
                btn = driver.find_element(By.XPATH, "//ul[@class='pagination']//a[text()='>']")
                btn.click()
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.table-bordered tbody tr")))
            except (NoSuchElementException, ElementNotInteractableException):
                break
    finally:
        driver.quit()
    
    df = pd.DataFrame(all_data, columns=[
        "№", "Организатор", "Название объявления", "Способ закупки", "Вид предмета закупки",
        "Дата начала приема заявок", "Дата окончания приема заявок", "Кол-во лотов", "Сумма объявления", "Статус"
    ])
    ds = kwargs["ds"]
    csv_path = os.path.join(BASE_DIR, f"announcements_{ds}.csv")
    xlsx_path = os.path.join(BASE_DIR, f"announcements_{ds}.xlsx")
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    df.to_excel(xlsx_path, index=False)
    Variable.set("last_announcements_csv", csv_path)
    return csv_path

def parse_lots_data(**kwargs):
    driver = setup_driver()
    all_data = []
    try:
        driver.get("https://fms.ecc.kz/ru/subpriceoffer")
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.table-bordered tbody tr")))
        page_num = 1
        while True:
            rows = driver.find_elements(By.CSS_SELECTOR, "table.table-bordered tbody tr")
            for row in rows:
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) >= 12:
                    all_data.append([ 
                        page_num,
                        cols[0].text.strip(),
                        cols[1].text.strip(),
                        cols[2].text.strip(),
                        cols[3].text.strip(),
                        cols[4].text.strip(),
                        cols[5].text.strip(),
                        cols[6].text.strip(),
                        cols[7].text.strip(),
                        cols[8].text.strip(),
                        cols[9].text.strip(),
                        cols[10].text.strip(),
                        cols[11].text.strip(),
                    ])
            try:
                btn = driver.find_element(By.XPATH, "//ul[@class='pagination']//a[text()='>']")
                btn.click()
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.table-bordered tbody tr")))
                page_num += 1
            except (NoSuchElementException, ElementNotInteractableException):
                break
    finally:
        driver.quit()
    
    df = pd.DataFrame(all_data, columns=[
        "Страница", "№ лота", "Заказчик", "Наименование объявления", "Наименование ЛС и ИМН",
        "Характеристика", "Способ закупки", "Планируемый срок закупки (месяц)",
        "Кол-во единиц измерения", "Цена за единицу (тенге)", "Сумма", "Статус", "Кол-во заявок"
    ])
    ds = kwargs["ds"]
    csv_path = os.path.join(BASE_DIR, f"lots_data_{ds}.csv")
    xlsx_path = os.path.join(BASE_DIR, f"lots_data_{ds}.xlsx")
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    df.to_excel(xlsx_path, index=False)
    Variable.set("last_lots_csv", csv_path)
    return csv_path

parse_healthcare_task = PythonOperator(
    task_id="parse_healthcare_data",
    python_callable=parse_healthcare_data,
    dag=dag,
)
parse_participants_task = PythonOperator(
    task_id="parse_participants_data",
    python_callable=parse_participants_data,
    dag=dag,
)
parse_announcements_task = PythonOperator(
    task_id="parse_announcements_data",
    python_callable=parse_announcements_data,
    dag=dag,
)
parse_lots_task = PythonOperator(
    task_id="parse_lots_data",
    python_callable=parse_lots_data,
    dag=dag,
)

parse_healthcare_task >> [
    parse_participants_task,
    parse_announcements_task,
    parse_lots_task,
]
	