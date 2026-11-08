from airflow.hooks.base import BaseHook
from minio import Minio
import json
from io import BytesIO
from airflow.exceptions import AirflowNotFoundException
import csv
from io import StringIO
from airflow.providers.postgres.hooks.postgres import PostgresHook

BUCKET_NAME = "stock-market"

def _get_minio_client():
    minio = BaseHook.get_connection("minio")
    client = Minio(
        endpoint=minio.extra_dejson['endpoint_url'].split("//")[1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False
    )
    return client

def _get_stock_prices(url, symbol):
    import requests

    url = f"{url}{symbol}?metrics=high?&interval=1d&range=1y"
    api = BaseHook.get_connection("stock_api")
    response = requests.get(url, headers=api.extra_dejson['headers'])
    return json.dumps(response.json()['chart']['result'][0])

def _store_prices(stock):
    client = _get_minio_client()
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
    stock = json.loads(stock)
    symbol = stock['meta']['symbol']
    data = json.dumps(stock, ensure_ascii=False).encode('utf-8')
    objw = client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=f"{symbol}/prices.json",
        data=BytesIO(data),
        length=len(data)
    )
    return f'{objw.bucket_name}/{symbol}'

def _get_formatted_csv(path):
    client = _get_minio_client()
    prefix_name = f"{path.split('/')[1]}/formatted_prices/"
    objects = client.list_objects(BUCKET_NAME, prefix=prefix_name, recursive=True)
    for obj in objects:
        if obj.object_name.endswith('.csv'):
            return f"{BUCKET_NAME}/{obj.object_name}"
    return AirflowNotFoundException(f"No CSV file found in {prefix_name} in bucket {BUCKET_NAME}")

def _load_to_postgres(path):
    """Load the formatted CSV from MinIO into public.stock_market (replace, like the Astro SDK default)."""

    client = _get_minio_client()
    object_name = path.split('/', 1)[1]  # drop the bucket name prefix
    response = client.get_object(BUCKET_NAME, object_name)
    try:
        text = response.read().decode('utf-8')
    finally:
        response.close()
        response.release_conn()

    def num(value, cast):
        return cast(value) if value not in ('', None) else None

    rows = [
        (
            num(r['timestamp'], int),
            num(r['close'], float),
            num(r['high'], float),
            num(r['low'], float),
            num(r['open'], float),
            num(r['volume'], int),
            r['date'] or None,
        )
        for r in csv.DictReader(StringIO(text))
    ]

    hook = PostgresHook(postgres_conn_id='postgres')
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.stock_market (
                    timestamp BIGINT,
                    close DOUBLE PRECISION,
                    high DOUBLE PRECISION,
                    low DOUBLE PRECISION,
                    open DOUBLE PRECISION,
                    volume BIGINT,
                    date DATE
                )
                """
            )
            cur.execute("TRUNCATE TABLE public.stock_market")
            cur.executemany(
                "INSERT INTO public.stock_market (timestamp, close, high, low, open, volume, date) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                rows,
            )
        conn.commit()
    finally:
        conn.close()
    return len(rows)
