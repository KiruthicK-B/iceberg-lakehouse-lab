from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.edgemodifier import Label
from cosmos import DbtTaskGroup

from _common import DATABRICKS_CATALOG, TRINO_POOL, execution_config, profile_config, project_config, render_config_for, databricks_conn

PLATFORM = "flipkart"
SCHEMA = f"silver_{PLATFORM}"
UPSTREAM = Dataset(f"bronze://{PLATFORM}")
DOWNSTREAM = Dataset(f"silver://{PLATFORM}")

with DAG(
    dag_id=f"{PLATFORM}_silver",
    description=f"bronze_{PLATFORM} -> {SCHEMA} (dbt staging models, native {PLATFORM} field names)",
    schedule=[UPSTREAM],  # auto-runs when bronze_{PLATFORM} finishes (manual OR trigger) -- also
                          # always manually triggerable directly, independent of this
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1},
    tags=["ecommerce", PLATFORM, "silver", "downstream"],
) as dag:

    def _create_schema():
        conn = databricks_conn()
        cur = conn.cursor()
        cur.execute(f"CREATE CATALOG IF NOT EXISTS {DATABRICKS_CATALOG}")
        cur.fetchall()
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {DATABRICKS_CATALOG}.{SCHEMA}")
        cur.fetchall()

    create_schema = PythonOperator(task_id="create_schema", python_callable=_create_schema, pool=TRINO_POOL)

    dbt_transform = DbtTaskGroup(
        group_id="dbt_transform",
        project_config=project_config,
        profile_config=profile_config,
        render_config=render_config_for(f"path:models/staging/{PLATFORM}"),
        execution_config=execution_config,
        operator_args={"pool": TRINO_POOL},
    )

    layer_done = EmptyOperator(task_id="layer_done", outlets=[DOWNSTREAM])

    create_schema >> Label(f"{SCHEMA} pre-created") >> dbt_transform >> Label("3 staging models built") >> layer_done
