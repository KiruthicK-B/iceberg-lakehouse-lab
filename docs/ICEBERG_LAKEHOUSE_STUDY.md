# Apache Iceberg, Polaris, Trino & RustFS — A to Z

An open-source, vendor-neutral lakehouse stack. Where the `retail_project` Databricks work (sibling directory `../../`) used Delta Lake + Unity Catalog + a Databricks-native compute engine, this stack swaps every layer for an open alternative you can run entirely on your own laptop: **Iceberg** (table format) + **Polaris** (catalog) + **Trino** (query engine) + **RustFS** (object storage). The working Docker lab for this is in `../` (`iceberg-lakehouse-lab/`).

---

## Table of Contents

1. [The four layers, and why they're separate](#1-the-four-layers-and-why-theyre-separate)
2. [Apache Iceberg](#2-apache-iceberg)
3. [Apache Polaris](#3-apache-polaris)
4. [Trino](#4-trino)
5. [RustFS](#5-rustfs)
6. [How a query actually flows through all four](#6-how-a-query-actually-flows-through-all-four)
7. [This stack vs Databricks (Delta + Unity Catalog)](#7-this-stack-vs-databricks-delta--unity-catalog)
8. [Running the lab](#8-running-the-lab)

---

## 1. The four layers, and why they're separate

The historic "data lake" problem: dump files (CSV/Parquet) into object storage, no transactions, no schema enforcement, no concurrent-write safety — every engine reading the lake had to guess table state from file listings. The **lakehouse** architecture fixes this by inserting a **table format** layer between raw files and query engines, plus a **catalog** layer that tracks which table format version is "current." Splitting into four independent layers (storage / table format / catalog / query engine) — instead of one vendor-locked monolith — is the whole point of this stack:

```
┌─────────────┐     ┌──────────────┐     ┌────────────┐     ┌─────────────┐
│   Storage   │ ←── │ Table Format │ ←── │  Catalog   │ ←── │ Query Engine│
│  (RustFS)   │     │  (Iceberg)   │     │ (Polaris)  │     │  (Trino)    │
└─────────────┘     └──────────────┘     └────────────┘     └─────────────┘
  raw bytes/S3        Parquet files +      "what's the        SQL, joins,
                       metadata/manifest    current table      aggregations,
                       tracking             version + where    federation
                                             is it stored"
```

Any layer is swappable: Iceberg tables can sit on S3, RustFS, MinIO, GCS, Azure Blob, or local disk. Any REST-catalog-compatible engine (Trino, Spark, Flink, Dremio, StarRocks, Snowflake) can read the same Iceberg tables through the same Polaris catalog. This interchangeability is the core value proposition versus a single-vendor stack.

---

## 2. Apache Iceberg

### What problem it solves

A **table format** — a spec for how a set of Parquet (or ORC/Avro) files plus metadata files together constitute one logical, ACID-transactional, schema-and-partition-evolvable table. Without it, "a table" is just a folder of files and every reader has to independently figure out which files are current, what schema they're in, and how to avoid reading a half-written file mid-write.

### Architecture — the metadata tree

```
catalog points to  →  metadata.json (current table metadata)
                            │
                            ├── schema (current + all historical schema versions)
                            ├── partition spec (current + historical)
                            ├── snapshot log (every past snapshot = a version of the table)
                            └── current-snapshot-id
                                      │
                                      ▼
                              manifest list (one per snapshot)
                                      │
                                      ▼
                              manifest files (list which data files belong
                                              to this snapshot, with stats:
                                              row counts, column min/max)
                                      │
                                      ▼
                              data files (actual Parquet files)
```

Every write produces a **new snapshot** (new metadata.json, new manifest list) — the old snapshot's files aren't touched or deleted, just no longer pointed to by "current." This is what gives Iceberg (and Delta Lake, which uses a similar but not identical scheme) **time travel** (`SELECT * FROM t FOR VERSION AS OF 123`) and **safe concurrent readers** (a reader that started before a write completes just keeps reading the old, complete snapshot — never sees a half-written state).

### Key features (intermediate level)

- **Schema evolution** — add/drop/rename/reorder columns, widen types, without rewriting existing data files. Old data files are read using the schema they were written with, reconciled against current schema by column ID (not column name/position) — this is why Iceberg tracks schema by stable field IDs internally, the actual mechanism that makes rename-without-rewrite safe.
- **Partition evolution** — change partitioning scheme (e.g. from daily to monthly) going forward, without rewriting historical data. Old files keep their old partition layout; the engine's query planner understands both layouts simultaneously.
- **Hidden partitioning** — you write `PARTITIONED BY (days(event_time))`, users query `WHERE event_time > '2026-01-01'` — Iceberg automatically derives the partition filter, no need for users to know or write partition-column predicates manually (unlike old Hive-style tables where you had to filter on the literal partition column).
- **Row-level deletes (format v2)** — two strategies: **position deletes** (mark specific row positions in specific files as deleted) and **equality deletes** (mark all rows matching a predicate as deleted, resolved at read time). This is Iceberg's version of Delta's "deletion vectors" — merge-on-read instead of rewriting whole files for every DELETE/UPDATE/MERGE.
- **Catalog abstraction** — Iceberg itself doesn't mandate *which* catalog tracks "current metadata.json pointer per table." Multiple catalog implementations exist: Hive Metastore, AWS Glue, Nessie (git-like branching catalog), JDBC catalog, and the **REST catalog spec** — which Polaris implements (section 3).

### Iceberg vs Delta Lake (comparing against what we used in the Databricks project)

| | Iceberg | Delta Lake |
|---|---|---|
| Origin | Netflix, donated to Apache | Databricks, donated to Linux Foundation |
| Metadata format | JSON + Avro manifests | JSON + Parquet checkpoint files |
| Catalog | Pluggable (REST spec is the modern standard) | Historically Databricks-centric (Unity Catalog); Delta also has an open protocol |
| Multi-engine support | Very strong — designed catalog-first for cross-engine interop from the start | Strong via Delta Lake open protocol, but historically strongest inside Databricks |
| Row-level deletes | Position + equality delete files | Deletion vectors (similar concept, different format) |
| Governed by | Apache Software Foundation | Linux Foundation (Delta Lake project), engine implementations vary |

In practice: if you're all-in on Databricks, Delta is the path of least resistance (what we did in `retail_project`). If you need genuine multi-engine, multi-vendor interoperability (Trino + Spark + Flink + Snowflake all reading the same physical tables without vendor lock-in), Iceberg + a REST catalog is the more neutral choice — which is exactly what this lab demonstrates.

### Problems / known pain points

- **Small file problem** — frequent small writes (streaming ingestion, many small batch jobs) produce many small Parquet files; query performance degrades (more files to open, more manifest entries to scan). Needs periodic **compaction** (`rewrite_data_files` procedure) — an operational task you must schedule, it doesn't happen automatically.
- **Manifest bloat** — over many snapshots, manifest lists/files accumulate; needs periodic **snapshot expiration** (`expire_snapshots`) to bound metadata size and let old data files actually get garbage collected.
- **Merge-on-read read-amplification** — equality/position deletes mean a read may have to merge base files with delete files at query time; heavy update/delete workloads without periodic compaction slow reads over time.
- **Catalog lock-in (historically)** — before the REST catalog spec matured, each catalog implementation (Glue, Hive Metastore, Nessie) had different governance/security models; migrating between them was non-trivial. The REST spec (which Polaris implements) is the industry's answer to this, but it's still a relatively young standard — not every engine's REST-catalog support is equally mature yet.
- **No engine of its own** — Iceberg is *only* a format/spec; every compute concern (query planning, joins, aggregation, streaming, ACID coordination for concurrent writers) is delegated to whatever engine (Trino, Spark, Flink) reads/writes it. Concurrent writer conflict resolution is optimistic-concurrency-based (retry on conflict) — high write-concurrency workloads on the same table can see real contention/retries.

---

## 3. Apache Polaris

### What it is

A **catalog** — the layer that answers "given a table name, what is its current metadata.json location, and is this caller allowed to read/write it." Polaris implements the **Iceberg REST Catalog API spec**, meaning any Iceberg-REST-compatible engine (Trino, Spark, Flink, Dremio, StarRocks — the same list Iceberg itself lists as REST-catalog clients) can talk to it identically, over plain HTTP + OAuth2, no engine-specific catalog plugin needed beyond "speak the REST spec."

Originally built and open-sourced by Snowflake, donated to the Apache Software Foundation (Apache Polaris, now an ASF project in its own right — not a Snowflake product).

### Architecture — Polaris's own RBAC model

Polaris layers a governance model *on top of* the Iceberg REST spec:

```
Realm                    (top-level tenant isolation, e.g. "POLARIS" in the lab)
  └── Catalog             (e.g. quickstart_catalog — maps to a storage location)
       └── Namespace       (like a schema, e.g. "demo")
            └── Table
  
Principal                 (an identity — a human or a service, e.g. root/s3cr3t in the lab)
  └── Principal Role       (a role assigned to principals)
       └── Catalog Role     (a role scoped to one catalog, holds actual grants)
            └── Grant        (e.g. CATALOG_MANAGE_CONTENT, TABLE_READ_DATA, etc.)
```

This is conceptually the same shape as the RBAC model covered in the earlier study guide's Unity Catalog RBAC section (`../../docs/STUDY_GUIDE.md`, section 1) — principal → role → grant → resource — just implemented independently for an open, multi-engine context instead of being Databricks-specific.

### Credential vending (the important security feature)

Instead of every query engine holding its own static, long-lived S3 credentials, Polaris can **vend short-lived, scoped credentials** per request — when Trino asks Polaris for a table's location, Polaris (which itself holds the real, long-lived storage credentials) returns a temporary, narrowly-scoped credential just for that table's prefix, valid briefly. This is `iceberg.rest-catalog.vended-credentials-enabled=true` in our Trino catalog config — Trino never sees RustFS's real admin credentials, only short-lived vended ones. Same underlying security principle as the storage-credential/external-location pattern from the Databricks project, generalized to a multi-engine world.

### Problems / maturity notes

- **Younger project** — smaller community/ecosystem than Hive Metastore or Glue, fewer production war-stories publicly documented yet, APIs still evolving between releases.
- **In-memory metastore in the default quickstart** (what our lab uses) is explicitly not production-durable — Polaris supports a JDBC-backed persistence layer (Postgres, per the earlier search results showing common "Polaris + Postgres for persistence" tutorials) for anything beyond local experimentation; the lab's `polaris` service loses all catalog state on container restart unless you configure that.
- **Realm concept is a common confusion point for newcomers** — most single-tenant setups only need one realm ("POLARIS" in our lab), but the API surfaces it everywhere (`Polaris-Realm` header on every request), which reads as unnecessary ceremony until you understand it's there for genuine multi-tenant SaaS deployments.
- **OAuth2 client-credentials setup friction** — bootstrapping the first principal/token (the `obtain-token.sh` dance in our lab) is a real onboarding hurdle; the "How I (Barely) Survived Setting Up Polaris" blog post found during research for this doc is a genuine, commonly-echoed sentiment about first-time setup friction.

---

## 4. Trino

### What it is

A distributed, MPP (massively parallel processing) SQL query engine. Originated as Facebook's **Presto**, forked into **PrestoSQL**, renamed **Trino** in 2020 (to disambiguate from Meta's own "PrestoDB" fork, which continued separately) — worth knowing this history since "Presto" and "Trino" tutorials/docs online refer to closely related but now-diverged projects.

Trino has **no storage of its own** — pure compute. It queries data wherever it lives via **connectors**: Iceberg, Hive, PostgreSQL, MySQL, Kafka, Elasticsearch, and dozens more, including querying *across* multiple connectors in a single federated SQL query (e.g. join an Iceberg table with a live PostgreSQL table in one query — a genuinely powerful, somewhat unique capability).

### Architecture

```
Client (trino CLI, JDBC, Web UI)
        │
        ▼
   Coordinator          — parses SQL, plans query, splits work into stages/tasks,
                           tracks worker health, serves the Web UI
        │
        ├──► Worker 1 ─┐
        ├──► Worker 2 ─┤  execute tasks in parallel, each pulling "splits"
        └──► Worker N ─┘  (chunks of the source data) via the relevant connector
```

In our single-container lab, Coordinator and Worker are the same process (fine for learning; production Trino separates them across many worker nodes for real parallelism).

### Catalogs = configured connector instances

Trino's own notion of "catalog" is just a properties file naming a connector + its config — our `guides/trino/catalog/polaris.properties` *is* Trino's catalog definition, naming `connector.name=iceberg` and pointing at Polaris's REST endpoint. This is a different, Trino-specific meaning of "catalog" from Polaris's Iceberg-REST-spec catalog concept (section 3) — same word, two layers, easy to conflate when first learning this stack. Addressing in SQL is three-part: `catalog.schema.table` → in our lab, `polaris.demo.events`.

### Problems / operational notes

- **Memory-intensive for large joins/aggregations** — Trino's execution model holds a lot of intermediate state in worker memory; large unfiltered joins or high-cardinality `GROUP BY`s without tuning (`query.max-memory`, spill-to-disk config) can OOM workers on default settings.
- **No transaction coordination beyond what the connector provides** — Trino delegates ACID entirely to Iceberg (via Polaris); Trino itself doesn't add any additional consistency guarantee across, say, a federated multi-connector query touching both Iceberg and PostgreSQL in one statement — that's inherently not atomic across systems.
- **No native caching layer by default** — repeated identical queries re-read from the connector every time unless you deploy a separate caching layer (Alluxio, or Trino's own optional query result caching in newer versions) — a real cost/latency consideration at scale that beginners often don't anticipate.
- **Not a streaming engine** — batch/interactive SQL only; if you need continuous stream processing (the way Flink or Spark Structured Streaming do), Trino isn't the tool, even though it can query Iceberg tables that a streaming engine is concurrently writing to.
- **Connector-specific quirks leak through** — despite the unified SQL surface, features and performance characteristics differ meaningfully by connector; an Iceberg-specific optimization (partition pruning, file-stats-based skipping) doesn't automatically generalize to, say, the PostgreSQL connector in the same federated query.

---

## 5. RustFS

### What it is

An S3-API-compatible object storage server written in Rust, positioned as a drop-in **MinIO alternative** — same S3 API surface, so any tool that speaks S3 (AWS CLI, boto3, Iceberg's S3 file-io, Polaris's storage layer) works against it by just pointing the endpoint URL at RustFS instead of AWS.

**Backstory worth knowing**: MinIO changed its open-source licensing/feature-gating posture in recent years (moving some previously-free features behind commercial licensing, removing the web console from the open-source AGPL build in some releases), which is the direct market context driving interest in fully-open alternatives like RustFS — the search results describing RustFS explicitly as "a drop-in replacement for MinIO" reflect this.

### Architecture

- **Single-node single-disk (SNSD)** — what our lab uses (`RUSTFS_VOLUMES: /data`), zero erasure coding, no redundancy — fine for local dev/learning, not for anything you care about losing.
- **Distributed/erasure-coded mode** — multiple nodes, multiple volumes each (`RUSTFS_VOLUMES=/data/rustfs{0..3}` pattern from the search results), production-oriented redundancy — out of scope for this lab but the natural next step if this were a real deployment.
- Exposes two ports: **9000** (S3 API — what Iceberg/Polaris/Trino actually talk to) and **9001** (web console — human-facing bucket browser, what you'd open to visually confirm the Parquet/metadata files landed correctly after running the demo queries).

### Problems / maturity notes

- **Newer/smaller project than MinIO** — less battle-tested at scale, smaller community, fewer third-party integration guides — the tradeoff for avoiding MinIO's licensing changes.
- **Non-root container UID gotcha** — runs as UID 10001 inside the container; if you bind-mount a host directory instead of a named Docker volume, that host directory must be `chown`'d to UID 10001 first or you hit permission-denied errors (our lab avoids this by not bind-mounting — `RUSTFS_VOLUMES: /data` uses the container's own filesystem, ephemeral on `docker compose down`).
- **Production-readiness track record still building** — as a young project, the kind of long-tail operational edge cases (specific S3 API compatibility corner cases, behavior under heavy concurrent write load) that MinIO has had years to shake out are comparatively less proven for RustFS.

---

## 6. How a query actually flows through all four

Walking through `SELECT * FROM polaris.demo.events` after the lab's demo data is loaded:

1. **Trino CLI → Trino Coordinator** — parses the SQL, sees catalog `polaris`, looks up the `iceberg` connector config in `polaris.properties`.
2. **Trino → Polaris (REST, port 8181)** — "give me the current metadata for table `demo.events`" — includes the OAuth2 bearer token obtained via client-credentials flow (`CLIENT_ID`/`CLIENT_SECRET` in the Trino container's env).
3. **Polaris responds** — current `metadata.json` location in RustFS, plus (since `vended-credentials-enabled=true`) a short-lived, scoped S3 credential valid just for reading that table's file prefix.
4. **Trino → RustFS (S3 API, port 9000)** — using the vended credential, fetches `metadata.json` → manifest list → manifest files → determines which Parquet data files are relevant (applying any predicate pushdown/partition pruning from the query).
5. **Trino workers read the actual Parquet files from RustFS**, execute the scan/filter/aggregate, return results to the CLI.

Every step is a plain HTTP or S3-API call between independently-replaceable services — the architectural point made in section 1, now concrete.

---

## 7. This stack vs Databricks (Delta + Unity Catalog)

Direct comparison against the `retail_project` pipeline built earlier in this repo:

| Layer | Databricks stack (`../../`) | This lab |
|---|---|---|
| Storage | AWS S3 (real bucket, real IAM) | RustFS (local, S3-compatible) |
| Table format | Delta Lake | Apache Iceberg |
| Catalog | Unity Catalog | Apache Polaris |
| Query engine | Databricks serverless SQL / notebooks (Spark under the hood) | Trino |
| Orchestration | Databricks Jobs (cron) | none in this lab — manual SQL only |
| Governance model | UC privileges (`GRANT`/`REVOKE`, masking, row filters — see `../../docs/STUDY_GUIDE.md`) | Polaris principals/roles/grants (same shape, different implementation) |
| Deployment | Managed cloud service | Fully local Docker, zero cloud dependency |
| Vendor neutrality | Delta is open-protocol but ecosystem is Databricks-centric | Every component is independently swappable (any REST-catalog engine, any S3-compatible storage) |

Neither is strictly "better" — the Databricks stack gets you a managed, integrated experience (one login, one bill, dashboards/Genie/ML all pre-wired, as built in `retail_project`) at the cost of being more centered on one vendor's ecosystem. This lab's stack is the "assemble it yourself from open parts" path — more moving pieces to understand and operate (four separate services vs one managed platform), but nothing here requires any single vendor, and the exact same Iceberg tables could be read by Spark, Flink, or Snowflake tomorrow without any data migration — just point a different REST-catalog client at the same Polaris catalog.

---

## 8. Running the lab

See [`../README.md`](../README.md) for exact commands. Summary: `docker compose -f guides/storage/docker-compose.yml -f guides/catalog/docker-compose.yml -f guides/trino/docker-compose.yml up` from the `iceberg-lakehouse-lab/` directory, wait for Trino's `HTTP server started` log line, then `docker exec -it $(docker ps -q --filter name=trino) trino` to get a SQL shell against `polaris.demo.events` (or your own tables).

Configuration reproduced from the official [Apache Polaris quickstart guides](https://github.com/apache/polaris/tree/main/site/content/guides) (Apache License 2.0).
