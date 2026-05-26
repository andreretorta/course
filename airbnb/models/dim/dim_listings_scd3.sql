{{
    config(materialized='table')
}}

/*
  SCD Type 3 — Add New Column
  ──────────────────────────────────────────────────────────────
  Only one level of history is stored: the previous value is
  kept in a dedicated column alongside the current value.
  Older history beyond the previous state is lost.

  Use when: you need a simple "current vs. previous" comparison
  (e.g. did this listing change its price or room type?) without
  the complexity of a full Type 2 history table.

  Columns tracked for history: price, room_type.
*/

WITH all_versions AS (
    SELECT
        id                                                  AS listing_id,
        name                                                AS listing_name,
        room_type,
        REPLACE(price, '$', '') :: NUMBER(10, 2)            AS price,
        CASE
            WHEN minimum_nights = 0 THEN 1
            ELSE minimum_nights
        END                                                 AS minimum_nights,
        host_id,
        dbt_valid_from,
        dbt_valid_to
    FROM {{ ref('scd_raw_listings') }}
),

with_previous AS (
    SELECT
        listing_id,
        listing_name,
        room_type                                                                       AS current_room_type,
        LAG(room_type)  OVER (PARTITION BY listing_id ORDER BY dbt_valid_from)         AS previous_room_type,
        price                                                                           AS current_price,
        LAG(price)      OVER (PARTITION BY listing_id ORDER BY dbt_valid_from)         AS previous_price,
        minimum_nights,
        host_id,
        dbt_valid_from                                                                  AS effective_from,
        dbt_valid_to
    FROM all_versions
)

SELECT
    listing_id,
    listing_name,
    current_room_type,
    previous_room_type,
    current_price,
    previous_price,
    CASE
        WHEN current_price <> previous_price
          OR (current_price IS NOT NULL AND previous_price IS NULL)
        THEN TRUE
        ELSE FALSE
    END                                                     AS price_has_changed,
    minimum_nights,
    host_id,
    effective_from
FROM with_previous
WHERE dbt_valid_to IS NULL   -- current records only
