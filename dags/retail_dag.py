"""Retail Medallion pipeline: CSV (Raw) -> Silver -> Gold (star schema) -> Parquet on AWS S3."""
from datetime import datetime, timedelta
import os

import boto3
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine

# Konfiguracja połączenia z PostgreSQL (w Dockerze host.docker.internal wskazuje na maszynę hosta).
# Wartości można nadpisać zmiennymi środowiskowymi.
DB_USER = os.getenv("RETAIL_DB_USER", "postgres")
DB_PASSWORD = os.getenv("RETAIL_DB_PASSWORD", "postgres")
DB_HOST = os.getenv("RETAIL_DB_HOST", "host.docker.internal")
DB_PORT = os.getenv("RETAIL_DB_PORT", "5433")
DB_NAME = os.getenv("RETAIL_DB_NAME", "online_retail_db")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

CSV_NAME = "Online Retail.csv"

# AWS S3 (można nadpisać zmiennymi środowiskowymi z pliku .env)
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "kacper-retail-data-lake")
S3_REGION_NAME = os.getenv("S3_REGION_NAME", "eu-north-1")  # eu-north-1 = Sztokholm
S3_PREFIX = "gold/"


def get_engine():
    # Engine tworzony leniwie (w tasku), a nie przy parsowaniu DAG-a przez scheduler
    return create_engine(DATABASE_URL)


default_args = {
    'owner': 'kacper',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# --- ZADANIE 1: Warstwa Silver (Czyszczenie i ładowanie) ---
def process_silver_layer(**context):
    print("Początek przetwarzania warstwy Silver...")
    engine = get_engine()

    # Plik CSV leży obok pliku DAG-a (folder dags)
    base_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(base_dir, CSV_NAME)

    # Polski format CSV: separator ';', przecinek dziesiętny, kodowanie latin1.
    # Klucze wczytujemy jako tekst, żeby CustomerID nie zamienił się w float (17850.0).
    df = pd.read_csv(
        file_path,
        encoding="latin1",
        sep=";",
        decimal=",",
        dtype={"InvoiceNo": str, "StockCode": str, "CustomerID": str,
               "Description": str, "Country": str},
    )
    print(f"Wielkość danych przed czyszczeniem: {len(df)}")

    df_silver = df.copy()
    df_silver["InvoiceNo"] = df_silver["InvoiceNo"].astype(str).str.strip()
    df_silver["StockCode"] = df_silver["StockCode"].astype(str).str.strip()

    # Polski format daty: dzień.miesiąc.rok godzina:minuta (np. 01.12.2010 08:26)
    df_silver["InvoiceDate"] = pd.to_datetime(
        df_silver["InvoiceDate"].str.strip(), format="%d.%m.%Y %H:%M"
    )

    # Braki i typy
    df_silver["CustomerID"] = (
        pd.to_numeric(df_silver["CustomerID"], errors="coerce").fillna(0).astype(int)
    )
    # Zabezpieczenie: gdyby jakaś cena nadal miała przecinek jako tekst
    df_silver["UnitPrice"] = pd.to_numeric(
        df_silver["UnitPrice"].astype(str).str.replace(",", ".", regex=False)
    )
    df_silver["Quantity"] = pd.to_numeric(df_silver["Quantity"]).astype(int)
    df_silver["Description"] = (
        df_silver["Description"].str.strip().str.replace(r"\s*\.$", "", regex=True)
    )

    # Filtrowanie anomalii i zwrotów (faktury zaczynające się na C, ilości <= 0, ceny <= 0)
    condition = (
        (df_silver["Quantity"] > 0)
        & (df_silver["UnitPrice"] > 0)
        & (~df_silver["InvoiceNo"].str.startswith("C"))
    )
    df_silver = df_silver[condition].copy()
    df_silver["total_amount"] = (df_silver["Quantity"] * df_silver["UnitPrice"]).round(2)

    print(f"Wielkość danych po czyszczeniu: {len(df_silver)}")

    df_silver.to_sql(
        name="silver_sales",
        con=engine,
        if_exists="replace",
        index=False,
        chunksize=10000,
    )
    print("Sukces! Tabela silver_sales została załadowana do PostgreSQL.")


# --- ZADANIE 2: Warstwa Gold (Model Gwiazdy / Star Schema) ---
def process_gold_layer(**context):
    print("Rozpoczęcie budowy warstwy Gold (Model Gwiazdy)...")
    engine = get_engine()
    df_silver = pd.read_sql("SELECT * FROM silver_sales;", con=engine)
    df_silver["InvoiceDate"] = pd.to_datetime(df_silver["InvoiceDate"])

    # Wymiar klient
    dim_customer = (
        df_silver[["CustomerID", "Country"]]
        .drop_duplicates()
        .sort_values(["CustomerID", "Country"])
        .reset_index(drop=True)
    )
    dim_customer["CustomerKey"] = range(1, len(dim_customer) + 1)

    # Wymiar produkt: jeden wiersz na StockCode (ten sam kod bywa opisany różnie).
    # Bierzemy najczęstszy niepusty opis dla danego kodu.
    desc_by_code = (
        df_silver.dropna(subset=["Description"])
        .groupby("StockCode")["Description"]
        .agg(lambda s: s.value_counts().idxmax())
    )
    dim_product = pd.DataFrame({"StockCode": df_silver["StockCode"].unique()})
    dim_product = dim_product.sort_values("StockCode").reset_index(drop=True)
    dim_product["Description"] = dim_product["StockCode"].map(desc_by_code)
    dim_product["ProductKey"] = range(1, len(dim_product) + 1)

    # Wymiar data
    df_silver["DateOnly"] = df_silver["InvoiceDate"].dt.normalize()
    dim_date = pd.DataFrame({"FullDate": df_silver["DateOnly"].unique()})
    dim_date = dim_date.sort_values("FullDate").reset_index(drop=True)
    dim_date["DateKey"] = range(1, len(dim_date) + 1)
    dim_date["Year"] = dim_date["FullDate"].dt.year
    dim_date["Month"] = dim_date["FullDate"].dt.month
    dim_date["Day"] = dim_date["FullDate"].dt.day
    dim_date["Quarter"] = dim_date["FullDate"].dt.quarter
    dim_date["DayOfWeek"] = dim_date["FullDate"].dt.day_name()

    # Tabela faktów: łączymy po kluczach naturalnych (StockCode, a nie StockCode+Description,
    # żeby nie gubić wierszy przy różnych opisach tego samego produktu)
    fact_sales = df_silver.merge(dim_customer, on=["CustomerID", "Country"], how="left")
    fact_sales = fact_sales.merge(dim_product[["StockCode", "ProductKey"]], on="StockCode", how="left")
    fact_sales = fact_sales.merge(
        dim_date[["FullDate", "DateKey"]], left_on="DateOnly", right_on="FullDate", how="left"
    )
    fact_sales["Total_Amount"] = fact_sales["total_amount"]

    fact_sales_final = fact_sales[[
        "InvoiceNo", "CustomerKey", "ProductKey", "DateKey",
        "Quantity", "UnitPrice", "Total_Amount",
    ]].copy()

    # Kontrola jakości: żaden klucz obcy nie może być pusty, liczba wierszy musi się zgadzać
    assert len(fact_sales_final) == len(df_silver), "Zmieniła się liczba wierszy faktów (duplikaty w wymiarze?)"
    assert not fact_sales_final[["CustomerKey", "ProductKey", "DateKey"]].isna().any().any(), \
        "Brak dopasowania klucza w tabeli faktów"

    dim_date["FullDate"] = dim_date["FullDate"].dt.date

    tables_to_load = {
        "dim_customer": dim_customer[["CustomerKey", "CustomerID", "Country"]],
        "dim_product": dim_product[["ProductKey", "StockCode", "Description"]],
        "dim_date": dim_date[["DateKey", "FullDate", "Year", "Month", "Day", "Quarter", "DayOfWeek"]],
        "fact_sales": fact_sales_final,
    }

    for table_name, table_df in tables_to_load.items():
        table_df.to_sql(
            name=table_name,
            con=engine,
            if_exists="replace",
            index=False,
            chunksize=10000,
        )
        print(f"Sukces! Tabela Gold '{table_name}' ({len(table_df)} wierszy) załadowana do PostgreSQL.")


# --- ZADANIE 3: Eksport do Parquet i wysyłka na AWS S3 ---
def export_to_aws_s3(**context):
    BUCKET_NAME = S3_BUCKET_NAME
    REGION_NAME = S3_REGION_NAME

    # Klucze AWS pochodzą z pliku .env przekazanego do kontenera (docker-compose -> env_file).
    # NIE wpisuj ich na sztywno w kodzie.
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    if not access_key or not secret_key:
        raise RuntimeError(
            "Brak AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY w kontenerze Airflow. "
            "Dopisz je do .env, potem: docker compose down && docker compose up -d"
        )

    engine = get_engine()
    s3_client = boto3.client(
        "s3",
        region_name=REGION_NAME,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    tables = ["dim_customer", "dim_product", "dim_date", "fact_sales"]

    tmp_dir = "/tmp/temp_parquet"
    os.makedirs(tmp_dir, exist_ok=True)

    for table_name in tables:
        print(f"--- Przetwarzanie tabeli do S3: {table_name} ---")
        df = pd.read_sql(f"SELECT * FROM {table_name};", con=engine)

        local_file_path = os.path.join(tmp_dir, f"{table_name}.parquet")
        df.to_parquet(local_file_path, index=False, engine="pyarrow")

        s3_key = f"{S3_PREFIX}{table_name}.parquet"
        print(f"Wysyłanie do S3: s3://{BUCKET_NAME}/{s3_key}...")

        try:
            s3_client.upload_file(local_file_path, BUCKET_NAME, s3_key)
            print(f"Sukces! Tabela {table_name} wysłana na S3 w formacie Parquet.\n")
        except Exception as e:
            print(f"Błąd podczas wysyłania {table_name} na S3: {e}\n")
            raise
        finally:
            if os.path.exists(local_file_path):
                os.remove(local_file_path)


with DAG(
    dag_id='retail_medallion_s3_pipeline',
    default_args=default_args,
    description='Pipeline ETL: Raw -> Silver -> Gold Star Schema -> AWS S3 Parquet',
    schedule=None,  # Uruchamiany ręcznie lub np. '@daily'
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['data-engineering', 'medallion', 'aws', 'postgres', 'airflow'],
) as dag:

    t_silver = PythonOperator(task_id='run_silver_layer', python_callable=process_silver_layer)
    t_gold = PythonOperator(task_id='run_gold_layer', python_callable=process_gold_layer)
    t_s3 = PythonOperator(task_id='upload_gold_to_s3', python_callable=export_to_aws_s3)

    t_silver >> t_gold >> t_s3
