from __future__ import annotations

import json
from datetime import datetime, timezone

import requests
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label

from _common import DATABRICKS_CATALOG, SHOPIFY_SOURCES, BATCH_SIZE, RUSTFS_BUCKET, TRINO_POOL, s3_client, databricks_conn

PLATFORM = "shopify"
BRONZE_SCHEMA = f"bronze_{PLATFORM}"
BRONZE_READY = Dataset(f"bronze://{PLATFORM}")


@dag(
    dag_id=f"{PLATFORM}_bronze_ingestion",
    description=f"{PLATFORM.title()} mock API -> RustFS raw landing -> bronze_{PLATFORM} Iceberg tables",
    schedule=None,  # manual trigger only -- root of the chain, nothing upstream to gate it
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1},
    tags=["ecommerce", PLATFORM, "bronze", "upstream"],
)
def bronze_ingestion():

    @task(pool=TRINO_POOL)
    def create_bronze_schema():
        # Run once, serially, before any load_bronze task -- keeps schema
        # creation idempotent and out of the way of concurrent load_bronze
        # instances.
        conn = databricks_conn()
        cur = conn.cursor()
        cur.execute(f"CREATE CATALOG IF NOT EXISTS {DATABRICKS_CATALOG}")
        cur.fetchall()
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {DATABRICKS_CATALOG}.{BRONZE_SCHEMA}")
        cur.fetchall()

    @task
    def land_raw(source: dict, **context) -> dict:
        resp = requests.get(source["url"], params={"n": source["n"]}, timeout=30)
        resp.raise_for_status()
        records = resp.json()

        key = f"raw-landing/{source['platform']}/{source['entity']}/{context['run_id']}.json"
        s3_client().put_object(
            Bucket=RUSTFS_BUCKET,
            Key=key,
            Body=json.dumps(records).encode("utf-8"),
        )
        print(f"Landed {len(records)} {source['platform']}.{source['entity']} records at s3://{RUSTFS_BUCKET}/{key}")
        return {**source, "key": key}

    @task(pool=TRINO_POOL)
    def load_bronze(landed: dict):
        platform, entity, key, columns = landed["platform"], landed["entity"], landed["key"], landed["columns"]
        table = f"{DATABRICKS_CATALOG}.{BRONZE_SCHEMA}.{platform}_{entity}_raw"
        col_names = [c[0] for c in columns]

        obj = s3_client().get_object(Bucket=RUSTFS_BUCKET, Key=key)
        records = json.loads(obj["Body"].read())

        conn = databricks_conn()
        cur = conn.cursor()

        col_defs = ", ".join(f"{name} {sql_type}" for name, sql_type in columns)
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                {col_defs},
                ingestion_timestamp TIMESTAMP
            )
        """)
        cur.fetchall()

        ingestion_ts = datetime.now(timezone.utc)
        rows = [tuple(r[c] for c in col_names) + (ingestion_ts,) for r in records]

        row_placeholders = "(" + ", ".join(["?"] * (len(col_names) + 1)) + ")"
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i : i + BATCH_SIZE]
            insert_sql = f"INSERT INTO {table} VALUES {', '.join([row_placeholders] * len(batch))}"
            flat_params = [value for row in batch for value in row]
            cur.execute(insert_sql, flat_params)
            cur.fetchall()

        print(f"Loaded {len(records)} rows into {table}")

    schema_ready = create_bronze_schema()
    landed = land_raw.expand(source=SHOPIFY_SOURCES)
    loaded = load_bronze.expand(landed=landed)
    bronze_done = EmptyOperator(task_id="bronze_done", outlets=[BRONZE_READY])

    landed.operator >> Label("raw JSON landed in RustFS") >> loaded.operator
    schema_ready >> Label(f"{BRONZE_SCHEMA} pre-created") >> loaded.operator
    loaded.operator >> Label("all 3 bronze tables loaded") >> bronze_done


bronze_ingestion()
