{{
    config(materialized='table')
}}

/*
  SCD Type 4 — Mini-Dimension / Separate History Table
  (current dimension)
  ──────────────────────────────────────────────────────────────
  SCD Type 4 splits data into two tables:
    • dim_listings_scd4         → current records only (this model)
    • dim_listings_scd4_history → full audit trail of all versions

  This table is the "hot path" for most analytical queries.
  Join it to dim_listings_scd4_history via history_key when the
  full change log is needed.

  Use when: most queries only need the current state but occasional
  audit queries require the complete history. The separation keeps
  the main dimension small and fast.
*/

WITH current_records AS (
    SELECT *
    FROM {{ ref('scd_raw_listings') }}
    WHERE dbt_valid_to IS NULL
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
    dbt_valid_from                                      AS effective_from,
    created_at,
    dbt_updated_at                                      AS updated_at
FROM current_records
