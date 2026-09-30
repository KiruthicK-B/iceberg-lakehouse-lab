# Iceberg lakehouse lab → Databricks-backed medallion pipeline

Started as a fully local Docker Compose lab built on the official [Apache Polaris "Getting Started with Trino"](https://polaris.apache.org/guides/trino/) quickstart. The orchestration/transformation stack (Airflow + dbt + Astronomer Cosmos) stayed exactly as designed; the storage/catalog/compute layer was later migrated from local Trino+Polaris+RustFS to a real **Databricks** workspace (Unity Catalog + SQL Warehouse). RustFS is still used, but only as a lightweight local raw-landing/staging area before Bronze load — not as the table storage layer anymore.

## Core services

- **RustFS** — S3-compatible object storage (Rust-based MinIO alternative). Used only for the `land_raw` staging step (raw JSON dumped before Bronze load) — the actual Iceberg/Delta tables live in Databricks now.
- **Databricks** — Unity Catalog (governance/metadata, catalog `ecommerce`) + a SQL Warehouse (compute). Real cloud service, not run via Docker Compose — connection details supplied via env vars (see "Databricks setup" below).
- **dashboard-proxy** — small `nginx` container that lets the browser-side `dashboard.html` reach Databricks' SQL Statement Execution API. Injects the real auth token server-side; the browser never sees it.

Full concept explanations (Iceberg/Polaris/Trino/RustFS background, from when this lab ran fully local) are in [`docs/ICEBERG_LAKEHOUSE_STUDY.md`](docs/ICEBERG_LAKEHOUSE_STUDY.md) — still accurate for the concepts, just not the current deployment target.

## The pipeline — 12 isolated DAGs, one per platform × layer

Three fully independent mock e-commerce platforms (Amazon/Shopify/Flipkart), each with its own schema chain: **Bronze** (raw ingestion) → **Silver** (per-platform normalized, native field names) → **Enterprise Silver** (relationship/FK-validated within that platform only) → **Gold** (star schema: `fact_<platform>_orders` + `dim_<platform>_users` + `dim_<platform>_items`). No cross-platform merging anywhere.

Each of the 4 layers × 3 platforms is its **own Airflow DAG** (12 total: `amazon_bronze_ingestion`, `amazon_silver`, `amazon_silver_enterprise`, `amazon_gold`, and the same 4 for `shopify_`/`flipkart_`), chained with **Airflow Datasets** rather than a fixed trigger chain — each layer's DAG updates an outlet `Dataset` on success, and the next layer's DAG has `schedule=[Dataset(...)]`. This means:
- Triggering Bronze cascades forward through the whole chain automatically.
- Triggering any layer directly (e.g. Silver) cascades forward to everything downstream of it, but never re-triggers what's upstream.
- Every DAG is also always independently manually-triggerable.

Tags on each DAG: `ecommerce`, the platform (`amazon`/`shopify`/`flipkart`), the layer (`bronze`/`silver`/`enterprise-silver`/`gold`), and `upstream`/`downstream` — filterable from the Airflow UI's DAG-list tag dropdown.

All 27 dbt models (`guides/dbt/ecommerce/models/**`) run via **Astronomer Cosmos**, using `dbt-databricks` as the adapter, each layer's DAG scoping `RenderConfig(select=...)` to just that platform+layer's 3 models.

## Databricks setup

Two env files, both gitignored, need the same 3 values from your Databricks workspace (SQL Warehouse → Connection details):

**`guides/airflow/.env`** (used by Airflow/dbt):
```
DATABRICKS_HOST=<workspace-hostname>            # e.g. dbc-xxxxxxxx-yyyy.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/<id>
DATABRICKS_TOKEN=<personal-access-token>
```

**`guides/dashboard-proxy/.env`** (used by the dashboard's CORS proxy — only host+token needed, no http_path):
```
DATABRICKS_HOST=<same-workspace-hostname>
DATABRICKS_TOKEN=<same-token>
```

Both `docker-compose.yml` files fail fast with a clear error if these aren't set, rather than starting with broken config.

## Run it

From this directory (`iceberg-lakehouse-lab/`):

```bash
# 1. RustFS (raw-landing staging only)
docker compose -f guides/storage/docker-compose.yml up -d

# 2. Mock APIs
docker compose -f guides/amazon-api/docker-compose.yml up -d --build
docker compose -f guides/shopify-api/docker-compose.yml up -d --build
docker compose -f guides/flipkart-api/docker-compose.yml up -d --build

# 3. Airflow (picks up dbt-databricks + astronomer-cosmos deps, needs guides/airflow/.env filled in)
docker compose -f guides/airflow/docker-compose.yml build
docker compose -f guides/airflow/docker-compose.yml up -d

# 4. Dashboard CORS proxy (needs guides/dashboard-proxy/.env filled in)
docker compose -f guides/dashboard-proxy/docker-compose.yml up -d
```

Trigger one platform's full chain — unpause once, trigger the root, everything downstream cascades via Datasets:
```bash
docker exec airflow-airflow-scheduler-1 airflow dags unpause amazon_bronze_ingestion
docker exec airflow-airflow-scheduler-1 airflow dags unpause amazon_silver
docker exec airflow-airflow-scheduler-1 airflow dags unpause amazon_silver_enterprise
docker exec airflow-airflow-scheduler-1 airflow dags unpause amazon_gold
docker exec airflow-airflow-scheduler-1 airflow dags trigger amazon_bronze_ingestion
```
(repeat the same 4-DAG unpause set for `shopify_`/`flipkart_` prefixes)

Query the result — **3 fully independent per-platform schemas under one Unity Catalog catalog (`ecommerce`), no cross-platform merging anywhere**. Each platform keeps its own native field names all the way to Gold:
```sql
-- Platform Silver (native Amazon field names — customer_id, asin, not renamed)
SELECT * FROM ecommerce.silver_amazon.stg_amazon__users LIMIT 5;

-- Per-platform Enterprise Silver (relationship-validated within that platform only)
SELECT count(*) FROM ecommerce.silver_amazon_enterprise.ent_amazon__orders;

-- Amazon Gold star schema — real FK join, Amazon-native columns
SELECT f.order_id, u.full_name, i.title, f.amount_usd
FROM ecommerce.gold_amazon.fact_amazon_orders f
JOIN ecommerce.gold_amazon.dim_amazon_users u ON f.customer_id = u.customer_id
JOIN ecommerce.gold_amazon.dim_amazon_items i ON f.asin = i.asin
LIMIT 10;

-- Same shape repeats independently for Flipkart, with Flipkart-native columns
SELECT f.order_no, u.name, i.product_name, f.amount_inr
FROM ecommerce.gold_flipkart.fact_flipkart_orders f
JOIN ecommerce.gold_flipkart.dim_flipkart_users u ON f.user_id = u.user_id
JOIN ecommerce.gold_flipkart.dim_flipkart_items i ON f.product_id = i.product_id
LIMIT 10;
```

`dashboard.html` (repo root) is a single self-contained file: an "ecommerce platform" dropdown (Amazon/Shopify/Flipkart) plus a Gold-table and filter dropdown, scoped to that platform's own native columns. No embedded data — every dropdown change fires a live query against Databricks' SQL Statement Execution API from the browser, through `guides/dashboard-proxy/` (a small `nginx` container that injects the real `Authorization: Bearer <token>` header server-side — the token never reaches the browser, only the local unauthenticated proxy does). Open `dashboard.html` directly in a browser; it talks to `http://localhost:8081`, so the proxy must be running.

## Useful URLs (once running)

| Service | URL | Credentials |
|---|---|---|
| Dashboard CORS proxy (for `dashboard.html`) | http://localhost:8081 | none (browser side) — real token injected server-side |
| RustFS Console | http://localhost:9001 | `rustfsadmin` / `rustfsadmin` |
| RustFS S3 API | http://localhost:9000 | `rustfsadmin` / `rustfsadmin` |
| Airflow UI | http://localhost:8090 | `admin` / `admin` |
| Databricks workspace | your workspace URL | your own login |
| amazon-api | http://localhost:8001 | none |
| shopify-api | http://localhost:8002 | none |
| flipkart-api | http://localhost:8003 | none |

## Databricks SQL dialect gotchas (vs the original Trino build)

dbt SQL is largely portable, but a few things weren't:
- `TIMESTAMP(6) WITH TIME ZONE` (Trino) → `TIMESTAMP` (Databricks — already tz-aware, no precision/suffix)
- `VARCHAR` with no length (Trino) → `STRING` (Databricks requires a length on bare `VARCHAR`)
- `date_parse(str, '%Y-%m-%dT%H:%i:%sZ')` (Trino, MySQL-style tokens) → `to_timestamp(str, "yyyy-MM-dd'T'HH:mm:ss'Z'")` (Databricks, Java pattern syntax)

## Directory layout

```
iceberg-lakehouse-lab/
├── README.md                          # this file
├── dashboard.html                     # single-file live dashboard (queries Databricks via dashboard-proxy)
├── docs/
│   └── ICEBERG_LAKEHOUSE_STUDY.md      # concept study guide (Iceberg/Polaris/Trino/RustFS background)
└── guides/
    ├── storage/docker-compose.yml      # rustfs service only -- raw-landing staging before Bronze load
    ├── dashboard-proxy/                # nginx CORS proxy: dashboard.html -> Databricks SQL Statement API
    │                                   # (.env holds DATABRICKS_HOST/TOKEN, injects auth server-side)
    ├── amazon-api/                     # enterprise pipeline: Amazon-flavored mock API
    ├── shopify-api/                    # enterprise pipeline: Shopify-flavored mock API
    ├── flipkart-api/                   # enterprise pipeline: Flipkart-flavored mock API
    ├── dbt/ecommerce/                  # dbt project (dbt-databricks adapter): 3 fully independent
    │                                   # per-platform pipelines (staging -> intermediate -> marts),
    │                                   # no cross-platform merging anywhere
    └── airflow/
        ├── docker-compose.yml          # postgres, webserver, scheduler (LocalExecutor)
        ├── .env                        # DATABRICKS_HOST/HTTP_PATH/TOKEN (gitignored)
        └── dags/
            ├── _common.py                          # shared constants/helpers, not a DAG itself
            ├── amazon_bronze_ingestion_dag.py       # + silver/silver_enterprise/gold (4 per platform)
            ├── shopify_bronze_ingestion_dag.py      # ...
            └── flipkart_bronze_ingestion_dag.py     # ... (12 DAG files total)
```

## Source

Core lakehouse configuration originally reproduced from the official Apache Polaris repository (Apache License 2.0): [`apache/polaris` — `site/content/guides/{rustfs,trino}`](https://github.com/apache/polaris/tree/main/site/content/guides) — since migrated to Databricks; the Polaris/Trino pieces of that quickstart are no longer part of this repo but the concept doc still describes them.
