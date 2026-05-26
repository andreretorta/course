# Airbnb dbt Project

An analytical data modelling project built on top of Airbnb data using **dbt** and **Snowflake**. The goal is to demonstrate data engineering best practices: layered architecture, data contracts, automated testing, historical tracking with SCD Types, and reusable macros.

> Portuguese version: [README.md](README.md)

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Data Sources](#2-data-sources)
3. [Project Structure](#3-project-structure)
4. [Model Layers](#4-model-layers)
   - [src — Staging](#41-src--staging-ephemeral)
   - [dim — Dimensions](#42-dim--dimensions-table)
   - [fct — Facts](#43-fct--facts-incremental)
   - [mart — Consumption](#44-mart--consumption-table)
5. [SCD Types](#5-scd-types)
6. [Snapshots](#6-snapshots)
7. [Seeds](#7-seeds)
8. [Macros](#8-macros)
9. [Tests](#9-tests)
10. [Analyses](#10-analyses)
11. [Getting Started](#11-getting-started)

---

## 1. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│  Snowflake — schema: RAW                                        │
│  raw_listings · raw_hosts · raw_reviews                         │
└──────────────────────────┬──────────────────────────────────────┘
                           │  source()
          ┌────────────────▼────────────────┐
          │  src/  (ephemeral)              │
          │  src_listings                   │
          │  src_hosts                      │
          │  src_reviews                    │
          └──────┬─────────────┬────────────┘
                 │             │
    ┌────────────▼──┐   ┌──────▼─────────────────┐
    │  dim/ (table) │   │  fct/ (incremental)     │
    │  dim_listings │   │  fct_reviews            │
    │  dim_hosts    │   └──────┬──────────────────┘
    │  dim_listing  │          │
    │  _w_hosts     │   ┌──────▼──────────────────┐
    │               │   │  mart/ (table)           │
    │  SCD 1,2,3,4  │   │  mart_fullmoon_reviews   │
    └───────────────┘   └─────────────────────────┘
          │
    ┌─────▼──────────────────┐
    │  snapshots/            │
    │  scd_raw_listings      │
    │  scd_raw_hosts         │
    └────────────────────────┘
```

### Why a layered architecture?

Each layer has a single, well-defined responsibility. Any change in the source data only affects the `src` layer, protecting all downstream business logic.

| Layer | Responsibility | Materialisation |
|---|---|---|
| `src` | Rename columns, basic typing | `ephemeral` (no physical table) |
| `dim` | Business logic, cleansing, joins | `table` |
| `fct` | Metrics and events | `incremental` |
| `mart` | Final output for BI/analytics consumption | `table` |

---

## 2. Data Sources

Defined in [`models/sources.yml`](models/sources.yml). All tables live in the `RAW` Snowflake schema.

| Source | Physical table | Description |
|---|---|---|
| `airbnb.listings` | `raw_listings` | Accommodation listings |
| `airbnb.hosts` | `raw_hosts` | Host account data |
| `airbnb.reviews` | `raw_reviews` | Guest reviews |

The `reviews` source has **freshness** configured: a warning is emitted if data is more than 1 hour old. This matters because `fct_reviews` is incremental — stale source data would cause silent gaps in the fact table.

```yaml
freshness:
  warn_after: {count: 1, period: hour}
```

---

## 3. Project Structure

```
airbnb/
├── analyses/               # Exploratory SQL (not materialised)
├── macros/                 # Reusable Jinja functions
├── models/
│   ├── src/                # Staging: direct source reads
│   ├── dim/                # Clean dimensions + SCD variants
│   ├── fct/                # Fact tables
│   ├── mart/               # Final consumption models
│   ├── schema.yml          # Tests and documentation for all models
│   └── sources.yml         # Source declarations
├── seeds/                  # CSV files versioned in the repository
├── snapshots/              # dbt-native SCD Type 2
├── tests/                  # Singular and custom generic tests
├── profiles.yml            # Credentials via env_var() — NOT committed
├── profiles.yml.example    # Public template for credential setup
└── dbt_project.yml         # Global project configuration
```

---

## 4. Model Layers

### 4.1 `src` — Staging (`ephemeral`)

**Files:** [`src_listings.sql`](models/src/src_listings.sql) · [`src_hosts.sql`](models/src/src_hosts.sql) · [`src_reviews.sql`](models/src/src_reviews.sql)

The `src` layer is the only one that calls `source()` to read raw data. It only renames columns to a standard naming convention (`id → listing_id`, `date → review_date`, `comments → review_text`) and selects relevant columns.

**Why `ephemeral`?** Ephemeral models produce no physical objects in the warehouse — they are compiled as inline CTEs inside the models that reference them. This avoids unnecessary staging tables while still providing code modularity.

```
raw_listings (source) → src_listings (ephemeral CTE) → dim_listings_cleansed (table)
```

---

### 4.2 `dim` — Dimensions (`table`)

**Base dimensions:**

| Model | Description |
|---|---|
| [`dim_listings_cleansed`](models/dim/dim_listings_cleansed.sql) | Listings with `minimum_nights` corrected (0→1) and `price` cast from string to NUMBER |
| [`dim_hosts_cleansed`](models/dim/dim_hosts_cleansed.sql) | Hosts with null names replaced by `'Anonymous'`. Has an active **data contract** |
| [`dim_listing_w_hosts`](models/dim/dim_listing_w_hosts.sql) | Denormalised wide dimension: join between listings and hosts |

**Why materialise as `table`?** Dimensions are queried at high frequency via joins in production. Materialising as a table avoids recomputing cleansing logic on every query.

**Data contract on `dim_hosts_cleansed`:**
```yaml
config:
  contract:
    enforced: true
```
With `contract: enforced`, dbt validates at runtime that the produced schema exactly matches the column types declared in `schema.yml`. If a source migration silently changes a column type, dbt fails before overwriting the table — protection against invisible breaking changes.

---

### 4.3 `fct` — Facts (`incremental`)

**File:** [`fct_reviews.sql`](models/fct/fct_reviews.sql)

Reviews fact table. Uses an **incremental** strategy: on each run, only records with a `review_date` greater than the current maximum in the table are inserted.

```sql
{% if is_incremental() %}
  AND review_date > (SELECT MAX(review_date) FROM {{ this }})
{% endif %}
```

**Why incremental?** The reviews table grows continuously. Reprocessing the full history on every run would be impractical in production — both in execution time and Snowflake compute cost.

**`on_schema_change='fail'`:** If a column is added or removed in the source, dbt aborts the run instead of silently creating a schema mismatch between old and new records.

---

### 4.4 `mart` — Consumption (`table`)

**File:** [`mart_fullmoon_reviews.sql`](models/mart/mart_fullmoon_reviews.sql)

The final layer consumed by BI tools and analysts. Combines `fct_reviews` with the `seed_full_moon_dates` seed to classify each review as `'full moon'` or `'not full moon'`, based on whether it was written the day after a full moon.

```sql
ON (TO_DATE(r.review_date) = DATEADD(DAY, 1, fm.full_moon_date))
```

The `DATEADD(DAY, 1, ...)` reflects the business hypothesis: the impact of a full moon on reviews would be felt the following day, not on the exact night.

---

## 5. SCD Types

Slowly Changing Dimensions (SCDs) solve the problem of **tracking changes in dimensional data over time**. For example: a host who changes their listing price — how do you record that without losing history?

The project implements all four classic strategies using `dim_listings` as the subject:

### Type 1 — Overwrite [`dim_listings_scd1`](models/dim/dim_listings_scd1.sql)
No history retained. Each run overwrites the previous value. Simple, but cannot answer "what was the price in January?".

**Use when:** disposable data, dashboards that only show the current state.

### Type 2 — Add New Row [`dim_listings_scd2`](models/dim/dim_listings_scd2.sql)
A new row is inserted for every change, with `valid_from` / `valid_to` and `is_current`. Can answer any historical question.

```
listing_id | price | valid_from  | valid_to    | is_current
1001       | $120  | 2024-01-01  | 2024-06-15  | FALSE
1001       | $150  | 2024-06-15  | NULL        | TRUE
```

**Use when:** full audit trail, point-in-time analysis. Powered by the `scd_raw_listings` snapshot.

### Type 3 — Add New Column [`dim_listings_scd3`](models/dim/dim_listings_scd3.sql)
Only the previous state is stored in dedicated extra columns (`previous_price`, `current_price`). One row per listing (always current). Includes a calculated `price_has_changed` flag.

```
listing_id | current_price | previous_price | price_has_changed
1001       | $150          | $120           | TRUE
```

**Use when:** simple "changed or not" reports, without needing deep historical records.

### Type 4 — Separate History Table [`dim_listings_scd4`](models/dim/dim_listings_scd4.sql) + [`dim_listings_scd4_history`](models/dim/dim_listings_scd4_history.sql)
Split into two tables: the main dimension holds only the current record (small, fast), while a separate history table stores all versions. Joined via `history_key`.

**Use when:** most queries only need the current state (performance), but occasional audits still require the full history. Best trade-off between performance and traceability.

---

## 6. Snapshots

**Files:** [`raw_listings_snapshot.yml`](snapshots/raw_listings_snapshot.yml) · [`raw_hosts_snapshot.yml`](snapshots/raw_hosts_snapshot.yml)

Snapshots are the foundation of dbt's **native SCD Type 2**. They read directly from sources and record every detected version, adding control columns: `dbt_scd_id`, `dbt_valid_from`, `dbt_valid_to`, `dbt_updated_at`.

```yaml
strategy: timestamp       # detects changes by comparing the updated_at field
unique_key: id
hard_deletes: invalidate  # source-deleted records are marked as invalid
```

`hard_deletes: invalidate` is critical: without it, a listing removed from Airbnb would remain with `is_current = TRUE` in the snapshot forever — silently generating incorrect data.

The SCD Type 2, 3 and 4 models all derive from these snapshots via `ref('scd_raw_listings')`.

---

## 7. Seeds

**File:** [`seeds/seed_full_moon_dates.csv`](seeds/seed_full_moon_dates.csv)

A CSV with full moon dates versioned directly in the repository. Used by the `mart_fullmoon_reviews` mart.

**Why a seed and not a table?** Full moon dates are static, deterministic reference data. Versioning them in the repository guarantees reproducibility across any environment (dev, prod, CI) without depending on an external data load.

---

## 8. Macros

Macros are **reusable Jinja functions** that prevent SQL logic duplication across models. Located in [`macros/`](macros/).

| Macro | Signature | Purpose |
|---|---|---|
| [`no_nulls_in_columns`](macros/no_nulls_in_columns.sql) | `(model)` | Generates a `SELECT` returning rows where any column is null. Useful in singular quality tests. |
| [`safe_divide`](macros/safe_divide.sql) | `(numerator, denominator)` | Division that returns `NULL` instead of an error when the denominator is 0 or null. |
| [`is_weekend`](macros/is_weekend.sql) | `(date_column)` | Returns `TRUE` if the date falls on a Saturday or Sunday. |
| [`clean_whitespace`](macros/clean_whitespace.sql) | `(column)` | Trims and collapses multiple internal spaces into one. |

**Example usage in SQL:**
```sql
SELECT
    {{ clean_whitespace('host_name') }}                AS host_name,
    {{ safe_divide('total_revenue', 'total_nights') }}  AS avg_revenue_per_night,
    {{ is_weekend('review_date') }}                     AS is_weekend_review
FROM {{ ref('fct_reviews') }}
```

**Why macros instead of inline SQL?** If the `safe_divide` logic needs to change (e.g. return `0` instead of `NULL`), the change happens in one place — the macro — and every model using it is updated automatically on the next `dbt compile`.

---

## 9. Tests

The project uses all four dbt test types:

### Schema Tests (declarative)
Defined in [`schema.yml`](models/schema.yml). Run automatically against declared columns.

```yaml
- name: listing_id
  data_tests:
    - not_null
    - unique
    - relationships:
        to: ref('dim_hosts_cleansed')
        field: host_id
```

### Singular Test
**File:** [`tests/dim_listings_minimum_nights.sql`](tests/dim_listings_minimum_nights.sql)

Returns rows where `minimum_nights < 1`. If any rows are returned, the test fails. Validates the business rule that every listing must require at least 1 night.

### Custom Generic Test
**File:** [`tests/generic/postivie_values.sql`](tests/generic/postivie_values.sql)

Implements the reusable `positive_values` test. Applied to `minimum_nights` in `dim_listings_cleansed` via `schema.yml`. Generalises the "value must be positive" validation to any model and column.

```sql
{% test positive_values(model, column_name) %}
  SELECT * FROM {{ model }} WHERE {{ column_name }} <= 0
{% endtest %}
```

### Unit Test
**File:** [`models/mart/unit_tests.yml`](models/mart/unit_tests.yml)

Tests `mart_fullmoon_reviews` logic with mocked data, without touching the database. Specifically validates that `DATEADD(DAY, 1, full_moon_date)` is applied correctly.

```yaml
given:
  - input: ref('fct_reviews')
    rows:
      - {review_date: '2025-01-15'}   # day after a full moon
expect:
  rows:
    - {review_date: '2025-01-15', is_full_moon: 'full moon'}
```

**Why all four types?** Each covers a different layer of confidence:
- Schema tests → data contract
- Singular tests → specific business rules
- Generic tests → reusable validations
- Unit tests → transformation logic isolated from the database

Test failures are stored in Snowflake (`data_tests: +store_failures: true` in `dbt_project.yml`), making it possible to inspect exactly which records failed.

---

## 10. Analyses

**File:** [`analyses/full_moon_no_sleep.sql`](analyses/full_moon_no_sleep.sql)

Exploratory SQL that aggregates reviews from `mart_fullmoon_reviews` by `is_full_moon` and `review_sentiment`. Not materialised — compiled to `target/compiled/` and executed manually when needed.

**Why use `analyses/` instead of running SQL directly?** The file participates in the dbt DAG (`ref()` works normally), is versioned in the repository, and can be shared with the team. It is the difference between an ad-hoc notebook and an auditable, reusable query.

---

## 11. Getting Started

### Step 0 — System prerequisites

#### Python
```bash
python --version   # requires 3.8+
```

#### Virtual environment and dbt installation
```bash
python -m venv venv

# Linux / macOS
source venv/bin/activate

# Windows (PowerShell)
.\venv\Scripts\Activate.ps1

pip install dbt-snowflake
dbt --version     # confirm the installation worked
```

---

### Step 1 — Set up Snowflake

Run the following as `ACCOUNTADMIN` in Snowflake to create all required objects:

```sql
-- Warehouse
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
  WITH WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 120
  AUTO_RESUME   = TRUE;

-- Database and schemas
CREATE DATABASE IF NOT EXISTS AIRBNB;
CREATE SCHEMA   IF NOT EXISTS AIRBNB.RAW;
CREATE SCHEMA   IF NOT EXISTS AIRBNB.DEV;
CREATE SCHEMA   IF NOT EXISTS AIRBNB.PROD;

-- Role and dbt user
CREATE ROLE IF NOT EXISTS TRANSFORM;
CREATE USER IF NOT EXISTS dbt
  DEFAULT_ROLE      = TRANSFORM
  DEFAULT_WAREHOUSE = COMPUTE_WH;

-- Permissions
GRANT ROLE TRANSFORM TO USER dbt;
GRANT USAGE  ON WAREHOUSE COMPUTE_WH    TO ROLE TRANSFORM;
GRANT USAGE  ON DATABASE  AIRBNB        TO ROLE TRANSFORM;
GRANT ALL    ON SCHEMA    AIRBNB.RAW    TO ROLE TRANSFORM;
GRANT ALL    ON SCHEMA    AIRBNB.DEV    TO ROLE TRANSFORM;
GRANT ALL    ON SCHEMA    AIRBNB.PROD   TO ROLE TRANSFORM;
GRANT SELECT ON ALL TABLES IN SCHEMA AIRBNB.RAW TO ROLE TRANSFORM;
GRANT SELECT ON FUTURE TABLES IN SCHEMA AIRBNB.RAW TO ROLE TRANSFORM;
```

The `RAW` schema must contain the three source tables:

| Table | Minimum expected columns |
|---|---|
| `raw_listings` | `id, name, listing_url, room_type, minimum_nights, host_id, price, created_at, updated_at` |
| `raw_hosts` | `id, name, is_superhost, created_at, updated_at` |
| `raw_reviews` | `listing_id, date, reviewer_name, comments, sentiment` |

---

### Step 2 — Key pair authentication

The project uses private key authentication instead of passwords. Generate your key pair:

```bash
# Generate an encrypted private key
openssl genrsa 2048 | openssl pkcs8 -topk8 -v2 aes256 -out rsa_key.p8

# Extract the public key
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

Register the public key in Snowflake:
```sql
-- Paste the content of rsa_key.pub without the -----BEGIN/END----- headers
ALTER USER dbt SET RSA_PUBLIC_KEY='MIIBIjANBgkqhki...';
```

> Keep `rsa_key.p8` in a secure location. Never commit this file.

---

### Step 3 — Clone and configure the project

```bash
git clone <repository-url>
cd airbnb
```

Create `profiles.yml` from the template:
```bash
cp profiles.yml.example profiles.yml
```

Set environment variables with your real credentials. The `SNOWFLAKE_ACCOUNT` value is your Snowflake account identifier (e.g. `abc12345.us-east-1`):

```bash
export SNOWFLAKE_ACCOUNT=abc12345.us-east-1
export SNOWFLAKE_USER=dbt
export SNOWFLAKE_ROLE=TRANSFORM
export SNOWFLAKE_WAREHOUSE=COMPUTE_WH
export SNOWFLAKE_DATABASE=AIRBNB
export SNOWFLAKE_PRIVATE_KEY_PASSPHRASE=your_key_password

# Full content of rsa_key.p8
export SNOWFLAKE_PRIVATE_KEY="$(cat rsa_key.p8)"
```

> `profiles.yml` is listed in `.gitignore` and **must never be committed**. Always use `profiles.yml.example` as the public reference.

To persist variables across sessions, add the `export` statements to your `~/.bashrc`, `~/.zshrc`, or your system's `.env` file.

---

### Step 4 — Install dbt dependencies

```bash
dbt deps
```

This installs the packages declared in `packages.yml` (e.g. `dbt-utils`) into the `dbt_packages/` folder.

---

### Step 5 — Verify the connection

```bash
dbt debug
```

All checks should show `OK`. If any fail, review your environment variables and Snowflake access permissions.

---

### Step 6 — Run the full pipeline

```bash
dbt build
```

`dbt build` executes **seed → run → snapshot → test** in the correct DAG order. On the first run:

1. Loads `seed_full_moon_dates.csv` into Snowflake
2. Creates all models (`src` as CTEs, `dim/mart` as tables, `fct` as incremental)
3. Runs SCD Type 2 snapshots (`scd_raw_listings`, `scd_raw_hosts`)
4. Executes all schema, singular, generic and unit tests

Expected output:
```
Completed successfully
Done. PASS=XX WARN=0 ERROR=0 SKIP=0 TOTAL=XX
```

---

### Step 7 — View documentation and lineage

```bash
dbt docs generate
dbt docs serve
```

Opens `http://localhost:8080` with interactive documentation and the full lineage graph of all models.

---

### Quick command reference

```bash
dbt seed                           # Reload seeds
dbt run                            # Models only (no tests/snapshots)
dbt snapshot                       # Update SCD Type 2 snapshots
dbt test                           # All tests
dbt test --select test_type:unit   # Unit tests only
dbt source freshness               # Check source freshness
dbt run --select dim+              # dim layer and all its dependants
dbt run --select +mart_fullmoon_reviews  # One model and all its ancestors
dbt build --target prod            # Production pipeline (PROD schema)
dbt clean                          # Remove target/ and dbt_packages/
```

### Available targets

| Target | Snowflake Schema | Threads | Use |
|---|---|---|---|
| `dev` (default) | `DEV` | 4 | Local development |
| `prod` | `PROD` | 8 | Production pipeline |
