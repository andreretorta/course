{{
    config(materialized='table')
}}

/*
  SCD Type 2 — Add New Row
  ──────────────────────────────────────────────────────────────
  A new row is inserted for every change. The previous row is
  closed with a valid_to timestamp. The currently active record
  always has valid_to = NULL (is_current = TRUE).

  Use when: full change history must be preserved so that queries
  can reconstruct the state of a listing at any point in time.

  Powered by the dbt snapshot: scd_raw_listings.
*/

WITH snapshot AS (
    SELECT * FROM {{ ref('scd_raw_listings') }}
)

SELECT
    dbt_scd_id                                          AS surrogate_key,
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
    dbt_valid_to                                        AS valid_to,
    CASE
        WHEN dbt_valid_to IS NULL THEN TRUE
        ELSE FALSE
    END                                                 AS is_current,
    created_at,
    dbt_updated_at                                      AS updated_at
FROM snapshot
