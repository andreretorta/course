{{
    config(materialized='table')
}}

/*
  SCD Type 4 — Mini-Dimension / Separate History Table
  (history table)
  ──────────────────────────────────────────────────────────────
  Stores ALL historical versions of every listing. Each row
  represents one period in which the listing held a particular
  set of attribute values.

  Open-ended records (currently active) use CURRENT_TIMESTAMP as
  valid_to so that range queries (BETWEEN valid_from AND valid_to)
  work uniformly across all rows.

  Join to dim_listings_scd4 via history_key to attach current
  attributes to an audit log row.
*/

WITH all_versions AS (
    SELECT * FROM {{ ref('scd_raw_listings') }}
)

SELECT
    dbt_scd_id                                          AS history_key,
    id                                                  AS listing_id,
    name                                                AS listing_name,
    room_type,
    CASE
        WHEN minimum_nights = 0 THEN 1
        ELSE minimum_nights
    END                                                 AS minimum_nights,
    host_id,
    REPLACE(price, '$', '') :: NUMBER(10, 2)            AS price,
    dbt_valid_from                                      AS valid_from,
    COALESCE(dbt_valid_to, CURRENT_TIMESTAMP)           AS valid_to,
    CASE
        WHEN dbt_valid_to IS NULL THEN TRUE
        ELSE FALSE
    END                                                 AS is_current,
    created_at,
    dbt_updated_at                                      AS updated_at
FROM all_versions
