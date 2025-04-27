from __future__ import annotations
from datetime import datetime, timedelta
import requests, os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

ckan_url = "https://data.opengov.kz"
api_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiJuU1BlaUhhazIwRVEtSGYtT2x0eTgyamFhT1p3MHJVWklFRGJpd1RJMkxjIiwiaWF0IjoxNzQ1MjcwNjYzfQ.cM26SPo3GlB8EpyXAC_jiNePqN1IpJpKITDYSzD2jLQ"
headers = {"Authorization": api_token}

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

dag = DAG(
    dag_id="upload_to_opengov_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
)

def _upload(csv_var, dataset_id, name, title, notes="", tags=""):
    csv_path = Variable.get(csv_var)
    with open(csv_path, "rb") as f:
        files = {"upload": (os.path.basename(csv_path), f, "application/octet-stream")}
        data = {
            "package_id": dataset_id,
            "name": name,
            "title": title,
            "notes": notes,
            "tags": tags,
        }
        r = requests.post(f"{ckan_url}/api/3/action/resource_create", headers=headers, files=files, data=data)
        r.raise_for_status()

upload_lots = PythonOperator(
    task_id="upload_lots",
    python_callable=_upload,
    op_kwargs={
        "csv_var": "last_lots_csv",
        "dataset_id": "nhqpopmaunr-o-jiotax-b-cqpepe-3dpabooxpahehnr",
        "name": "lots",
        "title": "Информация о лотах в сфере здравоохранения",
        "notes": "Этот набор данных содержит информацию о лотах, связанных с медицинскими закупками в Казахстане. Данные включают описание лотов, требования и условия участия.",
        "tags": "лоты, закупки, здравоохранение, Казахстан, медицинские закупки",
    },
    dag=dag,
)

upload_healthcare = PythonOperator(
    task_id="upload_healthcare_entities",
    python_callable=_upload,
    op_kwargs={
        "csv_var": "last_healthcare_csv",
        "dataset_id": "peectp-cy6bektob-3dpabooxpahehnr-yhactbyiownx-b-ocmc",
        "name": "healthcare_entities",
        "title": "Реестр субъектов здравоохранения",
    },
    dag=dag,
)

upload_announcements = PythonOperator(
    task_id="upload_announcements",
    python_callable=_upload,
    op_kwargs={
        "csv_var": "last_announcements_csv",
        "dataset_id": "o6brbjiehnr-o-3akynkax-b-cqpepe-3dpabooxpahehnr",
        "name": "announcements",
        "title": "Объявления о закупках в сфере здравоохранения",
        "notes": "Набор данных включает информацию о объявлениях, связанных с закупками в сфере здравоохранения в Казахстане. Включает данные о проводимых тендерах, условиях и сроках.",
        "tags": "объявления, закупки, здравоохранение, тендеры, Казахстан",
    },
    dag=dag,
)

upload_participants = PythonOperator(
    task_id="upload_participants",
    python_callable=_upload,
    op_kwargs={
        "csv_var": "last_participants_csv",
        "dataset_id": "peectp-yhacthnkob-tehdepob-b-cqpepe-3dpabooxpahehnr",
        "name": "participants",
        "title": "Реестр участников тендеров в сфере здравоохранения",
        "notes": "Этот набор данных содержит информацию о юридических лицах, участвующих в тендерах по медицинским закупкам в Казахстане. Включает данные о компаниях, их предложениях и статусах участия.",
        "tags": "участники, тендеры, закупки, здравоохранение, Казахстан, юридические лица",
    },
    dag=dag,
)
