"""Shared constants/helpers for the per-platform, per-layer ecommerce DAGs.

Not a DAG file itself (no `dag`/`DAG` object at module scope), so the
scheduler's DAG-file parser skips it after finding nothing to register.
"""
from __future__ import annotations

import os

import boto3
from databricks import sql as databricks_sql
from cosmos import ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, InvocationMode

RUSTFS_ENDPOINT = "http://rustfs:9000"
RUSTFS_BUCKET = "bucket123"
DATABRICKS_HOST = os.environ["DATABRICKS_HOST"]
DATABRICKS_HTTP_PATH = os.environ["DATABRICKS_HTTP_PATH"]
DATABRICKS_CATALOG = "ecommerce"
BATCH_SIZE = 50

# One shared pool across all 12 DAGs so total concurrent Databricks SQL
# Warehouse load stays bounded no matter how many platform chains happen to
# cascade at once -- max_active_tasks is scoped per-DAG and can't do this
# once the work that used to be one DAG is spread across many.
TRINO_POOL = "trino_pool"

DBT_PROJECT_DIR = "/opt/airflow/dbt/ecommerce"
# Pre-built venv (baked into the image, see Dockerfile) with dbt-core +
# dbt-databricks, fully isolated from Airflow's own site-packages.
DBT_EXECUTABLE = "/home/airflow/dbt_venv/bin/dbt"

# One dbt source/schema per platform now (bronze_amazon/bronze_shopify/
# bronze_flipkart) -- keeps Bronze symmetric with every later layer and
# removes any cross-DAG schema-creation race between platforms.
AMAZON_SOURCES = [
    {
        "platform": "amazon", "entity": "users", "url": "http://amazon-api:8000/users", "n": 150,
        "columns": [["customer_id", "BIGINT"], ["full_name", "STRING"], ["email_address", "STRING"],
                    ["signup_epoch", "BIGINT"], ["country", "STRING"]],
    },
    {
        "platform": "amazon", "entity": "items", "url": "http://amazon-api:8000/items", "n": 80,
        "columns": [["asin", "STRING"], ["title", "STRING"], ["category", "STRING"],
                    ["price_usd", "DOUBLE"], ["brand", "STRING"]],
    },
    {
        "platform": "amazon", "entity": "orders", "url": "http://amazon-api:8000/orders", "n": 200,
        "columns": [["order_id", "STRING"], ["customer_id", "BIGINT"], ["asin", "STRING"],
                    ["order_epoch", "BIGINT"], ["qty", "INTEGER"], ["unit_price_usd", "DOUBLE"],
                    ["order_status", "STRING"]],
    },
]

SHOPIFY_SOURCES = [
    {
        "platform": "shopify", "entity": "users", "url": "http://shopify-api:8000/users", "n": 150,
        "columns": [["cust_id", "BIGINT"], ["cust_email", "STRING"], ["display_name", "STRING"],
                    ["created_at", "STRING"], ["country_code", "STRING"]],
    },
    {
        "platform": "shopify", "entity": "items", "url": "http://shopify-api:8000/items", "n": 80,
        "columns": [["sku", "STRING"], ["product_title", "STRING"], ["product_type", "STRING"],
                    ["price", "DOUBLE"], ["vendor", "STRING"]],
    },
    {
        "platform": "shopify", "entity": "orders", "url": "http://shopify-api:8000/orders", "n": 200,
        "columns": [["shopify_order_id", "STRING"], ["cust_id", "BIGINT"], ["sku", "STRING"],
                    ["created_at", "STRING"], ["line_qty", "INTEGER"], ["total_price", "DOUBLE"],
                    ["financial_status", "STRING"]],
    },
]

FLIPKART_SOURCES = [
    {
        "platform": "flipkart", "entity": "users", "url": "http://flipkart-api:8000/users", "n": 150,
        "columns": [["user_id", "BIGINT"], ["name", "STRING"], ["email", "STRING"],
                    ["join_date", "STRING"], ["state", "STRING"]],
    },
    {
        "platform": "flipkart", "entity": "items", "url": "http://flipkart-api:8000/items", "n": 80,
        "columns": [["product_id", "STRING"], ["product_name", "STRING"], ["category", "STRING"],
                    ["mrp_inr", "DOUBLE"], ["seller", "STRING"]],
    },
    {
        "platform": "flipkart", "entity": "orders", "url": "http://flipkart-api:8000/orders", "n": 200,
        "columns": [["order_no", "STRING"], ["user_id", "BIGINT"], ["product_id", "STRING"],
                    ["order_date", "STRING"], ["quantity", "INTEGER"], ["amount_inr", "DOUBLE"],
                    ["status", "STRING"]],
    },
]

SOURCES_BY_PLATFORM = {
    "amazon": AMAZON_SOURCES,
    "shopify": SHOPIFY_SOURCES,
    "flipkart": FLIPKART_SOURCES,
}


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=RUSTFS_ENDPOINT,
        aws_access_key_id="rustfsadmin",
        aws_secret_access_key="rustfsadmin",
    )


def databricks_conn():
    return databricks_sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=os.environ["DATABRICKS_TOKEN"],
    )


profile_config = ProfileConfig(
    profile_name="ecommerce",
    target_name="dev",
    profiles_yml_filepath=f"{DBT_PROJECT_DIR}/profiles.yml",
)
project_config = ProjectConfig(dbt_project_path=DBT_PROJECT_DIR)
execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,
    invocation_mode=InvocationMode.SUBPROCESS,
    dbt_executable_path=DBT_EXECUTABLE,
)


def render_config_for(select_path: str) -> RenderConfig:
    """One RenderConfig per layer DAG, scoped to just that platform+layer's
    models via dbt's path selector -- e.g. "path:models/staging/amazon".
    emit_datasets=False: we hand-declare one dataset per layer (see each
    DAG's `layer_done` task) instead of Cosmos's default one-per-model
    datasets, which would be noisier than useful for gating a whole
    downstream DAG on.
    """
    return RenderConfig(
        dbt_executable_path=DBT_EXECUTABLE,
        invocation_mode=InvocationMode.SUBPROCESS,
        select=[select_path],
        emit_datasets=False,
    )
