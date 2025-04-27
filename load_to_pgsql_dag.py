from __future__ import annotations
from datetime import datetime, timedelta
import os, re, numpy as np, pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text

BASE_DIR = "/opt/airflow/parsed_data"
PG_CONN_STR = "postgresql+psycopg2://postgres:123@client-postgres:5432/medical_db"
engine = create_engine(PG_CONN_STR)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="load_to_pgsql_dag",
    start_date=datetime(2024, 1, 1),
    description="Полная загрузка CSV -> medical_db",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
)

def _get_id(table: str, name: str | None) -> int | None:
    if not name or pd.isna(name):
        return None
    with engine.connect() as c:
        r = c.execute(text(f"select id from {table} where name=:n"), {"n": name}).fetchone()
        return r[0] if r else None

def _bulk_upsert(df: pd.DataFrame, table: str, key_cols: list[str]):
    if df.empty:
        return
    with engine.begin() as c:
        exist = pd.read_sql(f"select * from {table}", c)
        mask = ~df.set_index(key_cols).index.isin(exist.set_index(key_cols).index)
        new_rows = df[mask]
        if not new_rows.empty:
            new_rows.to_sql(table, c, if_exists="append", index=False)
            print(f"{table}: +{len(new_rows)}")

def load_healthcare_dims(**_):
    csv = Variable.get("last_healthcare_csv")
    df = pd.read_csv(csv, encoding="utf-8-sig")
    load_map = {
        "regions": "Регион, населению которого будут оказываться услуги",
        "ownership_forms": "Форма собственности субъекта здравоохранения",
        "healthcare_entity_statuses": "Статус субъекта здравоохранения",
    }
    for tbl, col in load_map.items():
        _bulk_upsert(pd.DataFrame(df[col].dropna().unique(), columns=["name"]), tbl, ["name"])
    def _split(col1, col2, tbl_t, tbl_st):
        pairs = df[[col1, col2]].dropna().drop_duplicates()
        _bulk_upsert(pd.DataFrame(pairs[col1].unique(), columns=["name"]), tbl_t, ["name"])
        with engine.connect() as c:
            tmap = pd.read_sql(f"select id,name from {tbl_t}", c)
        pairs = pairs.merge(tmap, left_on=col1, right_on="name")
        sub = pairs[[col2, "id"]]
        sub.columns = ["name", f"{tbl_t[:-1]}_id"]
        _bulk_upsert(sub, tbl_st, ["name"])
    _split("Вид/Форма медицинской помощи (Поставщик)",
           "Подвида вида/формы медицинской помощи (Поставщик)",
           "provider_help_types", "provider_help_subtypes")
    _split("Вид/Форма медицинской помощи (Соисполнитель)",
           "Подвида вида/формы медицинской помощи (Соисполнитель)",
           "coexecutor_help_types", "coexecutor_help_subtypes")

def load_healthcare_fact(**_):
    csv = Variable.get("last_healthcare_csv")
    df = pd.read_csv(csv, encoding="utf-8-sig")
    le = df[["Наименование субъекта здравоохранения", "БИН/ИИН"]].drop_duplicates()
    le.columns = ["name", "bin"]
    le["organization_type"] = "Субъект здравоохранения"
    _bulk_upsert(le, "legal_entities", ["name", "bin"])
    df["legal_entity_id"] = df.apply(lambda r: _get_id("legal_entities", r["Наименование субъекта здравоохранения"]), axis=1)
    df["ownership_form_id"] = df["Форма собственности субъекта здравоохранения"].map(lambda x: _get_id("ownership_forms", x))
    df["region_id"] = df["Регион, населению которого будут оказываться услуги"].map(lambda x: _get_id("regions", x))
    df["entity_status_id"] = df["Статус субъекта здравоохранения"].map(lambda x: _get_id("healthcare_entity_statuses", x))
    df["is_long_term_contract"] = df["На заключение долгосрочного договора"].str.lower().eq("да")
    df["is_db_included"] = df["Статус"].str.contains("включ", case=False, na=False)
    fact = df[[
        "legal_entity_id", "is_long_term_contract", "ownership_form_id", "region_id",
        "Дата включения в базу данных", "Дата исключения из базы данных", "Дата последнего изменения",
        "entity_status_id", "is_db_included"
    ]].rename(columns={
        "Дата включения в базу данных": "date_inclusion",
        "Дата исключения из базы данных": "date_exclusion",
        "Дата последнего изменения": "date_change",
    })
    _bulk_upsert(fact.drop_duplicates(), "healthcare_details",
                 ["legal_entity_id", "date_inclusion", "entity_status_id"])

def load_ann_dims(**_):
    csv = Variable.get("last_announcements_csv")
    df = pd.read_csv(csv, encoding="utf-8-sig")
    for tbl, col in {
        "purchase_methods": "Способ закупки",
        "item_types": "Вид предмета закупки",
        "procurement_statuses": "Статус",
    }.items():
        _bulk_upsert(pd.DataFrame(df[col].dropna().unique(), columns=["name"]), tbl, ["name"])

def load_ann_fact(**_):
    csv = Variable.get("last_announcements_csv")
    df = pd.read_csv(csv, encoding="utf-8-sig")
    le_org = pd.DataFrame({
        "name": df["Организатор"].dropna().unique(),
        "organization_type": "Организатор",
        "bin": None,
    })
    _bulk_upsert(le_org, "legal_entities", ["name", "organization_type"])
    with engine.connect() as c:
        le = pd.read_sql("select id,name from legal_entities where organization_type='Организатор'", c)
    org = le.rename(columns={"id": "legal_entity_id"})[["legal_entity_id"]]
    _bulk_upsert(org, "organizers", ["legal_entity_id"])
    idmaps = {t: dict(pd.read_sql(f"select name,id from {t}", engine).values) for t in
              ("purchase_methods", "item_types", "procurement_statuses")}
    df_fact = pd.DataFrame({
        "organizer_id": df["Организатор"].map(lambda x: le.set_index("name").at[x, "id"]),
        "name": df["Название объявления"],
        "purchase_method_id": df["Способ закупки"].map(idmaps["purchase_methods"].get),
        "item_type_id": df["Вид предмета закупки"].map(idmaps["item_types"].get),
        "application_start": pd.to_datetime(df["Дата начала приема заявок"], errors="coerce"),
        "application_end": pd.to_datetime(df["Дата окончания приема заявок"], errors="coerce"),
        "lot_count": pd.to_numeric(df["Кол-во лотов"], errors="coerce"),
        "total_sum": pd.to_numeric(df["Сумма объявления"].astype(str).str.replace("[ ,]", "", regex=True), errors="coerce"),
        "status_id": df["Статус"].map(idmaps["procurement_statuses"].get),
    })
    _bulk_upsert(df_fact.dropna(subset=["organizer_id"]), "announcements",
                 ["organizer_id", "name"])

def load_lots_fact(**_):
    csv = Variable.get("last_lots_csv")
    df = pd.read_csv(csv, encoding="utf-8-sig")
    le_cust = pd.DataFrame({
        "name": df["Заказчик"].dropna().unique(),
        "organization_type": "Заказчик",
        "bin": None,
    })
    _bulk_upsert(le_cust, "legal_entities", ["name", "organization_type"])
    with engine.connect() as c:
        cust_le = pd.read_sql("select id,name from legal_entities where organization_type='Заказчик'", c)
    _bulk_upsert(cust_le.rename(columns={"id": "legal_entity_id"})[["legal_entity_id"]], "customers", ["legal_entity_id"])
    with engine.connect() as c:
        maps = {
            "ann": pd.read_sql("select id,name from announcements", c).set_index("name")["id"].to_dict(),
            "cust": cust_le.set_index("name")["id"].to_dict(),
            "meth": pd.read_sql("select id,name from purchase_methods", c).set_index("name")["id"].to_dict(),
            "stat": pd.read_sql("select id,name from procurement_statuses", c).set_index("name")["id"].to_dict(),
        }
    df_fact = pd.DataFrame({
        "announcement_id": df["Наименование объявления"].map(lambda x: maps["ann"].get(re.sub(r"Дата начала.*", "", str(x)).strip())),
        "customer_id": df["Заказчик"].map(maps["cust"].get),
        "medicine_name": df["Наименование ЛС и ИМН"],
        "purchase_method_id": df["Способ закупки"].map(maps["meth"].get),
        "plan_month": df["Планируемый срок закупки (месяц)"],
        "unit_quantity": df["Кол-во единиц измерения"],
        "price": pd.to_numeric(df["Цена за единицу (тенге)"], errors="coerce"),
        "total_sum": pd.to_numeric(df["Сумма"], errors="coerce"),
        "status_id": df["Статус"].map(maps["stat"].get),
        "application_count": pd.to_numeric(df["Кол-во заявок"], errors="coerce"),
    }).dropna(subset=["announcement_id", "customer_id", "purchase_method_id", "status_id"])
    _bulk_upsert(df_fact, "lots", ["announcement_id", "customer_id", "medicine_name"])

def load_participants(**_):
    csv = Variable.get("last_participants_csv")
    df = pd.read_csv(csv, encoding="utf-8-sig")
    le_part = df.rename(columns={
        "Наименование участника": "name",
        "БИН (ИНН, УНП)": "bin"
    })[["name", "bin"]].dropna(subset=["name"]).drop_duplicates()
    le_part["organization_type"] = "Участник"
    _bulk_upsert(le_part, "legal_entities", ["name", "bin"])
    with engine.connect() as c:
        ids = pd.read_sql("select id from legal_entities where organization_type='Участник'", c)
    _bulk_upsert(ids.rename(columns={"id": "legal_entity_id"}), "participants", ["legal_entity_id"])

t_hc_dims   = PythonOperator(task_id="dims_healthcare",      python_callable=load_healthcare_dims, dag=dag)
t_hc_fact   = PythonOperator(task_id="fact_healthcare",      python_callable=load_healthcare_fact, dag=dag)
t_ann_dims  = PythonOperator(task_id="dims_announcements",   python_callable=load_ann_dims,      dag=dag)
t_ann_fact  = PythonOperator(task_id="fact_announcements",   python_callable=load_ann_fact,      dag=dag)
t_lot_fact  = PythonOperator(task_id="fact_lots",            python_callable=load_lots_fact,     dag=dag)
t_part      = PythonOperator(task_id="load_participants",    python_callable=load_participants,  dag=dag)

t_hc_dims >> t_hc_fact >> t_part
[t_ann_dims, t_part] >> t_ann_fact >> t_lot_fact
