{{
    config(materialized='view')
}}

/*
  SCD Type 1 — Overwrite
  ──────────────────────────────────────────────────────────────
  No history is retained. Every dbt run reflects only the latest
  source values. Previous values are permanently lost.

  Use when: historical accuracy is not required and only the
  current snapshot of the data matters (e.g. BI dashboards that
  only show "today's" listings).
*/

SELECT * FROM {{ ref('dim_listings_cleansed') }}
