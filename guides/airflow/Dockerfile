FROM apache/airflow:2.10.3

# astronomer-cosmos itself + our own DAG code's light deps, in the main
# Airflow env. databricks-sql-connector is what the Bronze ingestion tasks
# use directly (not through dbt) to CREATE SCHEMA/TABLE and INSERT rows.
RUN pip install --no-cache-dir \
    databricks-sql-connector==4.6.0 \
    boto3==1.35.36 \
    astronomer-cosmos==1.15.1

# dbt-core + dbt-databricks in a fully separate, isolated venv -- installing
# them directly into the main env conflicts hard with Airflow's own
# dependency tree (opentelemetry version collisions via astronomer-cosmos ->
# apache-airflow, colliding with dbt-core's own transitive deps -- pip
# backtracks through 100+ dbt-adapters/dbt-protos releases trying to
# reconcile them).
# `virtualenv` (the PyPI package, not stdlib venv) is already present as a
# transitive dep of astronomer-cosmos, so no extra system packages needed.
# Built once at image-build time -- Cosmos needs a real `dbt` executable
# available immediately at DAG-parse time (for `dbt ls`), not just lazily at
# task-run time, so this can't be deferred to a runtime-built venv.
RUN python3 -m virtualenv /home/airflow/dbt_venv && \
    /home/airflow/dbt_venv/bin/pip install --no-cache-dir \
        dbt-core==1.9.11 \
        "dbt-databricks~=1.9.0"
