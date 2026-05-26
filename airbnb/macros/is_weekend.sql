{#
  is_weekend(date_column)

  Returns TRUE when the given date falls on a Saturday or Sunday.
  Useful for analysing whether reviews or bookings happen on weekends.

  Usage:
    {{ is_weekend('review_date') }} AS is_weekend_review
#}
{% macro is_weekend(date_column) %}
    DAYOFWEEK({{ date_column }}) IN (0, 6)
{% endmacro %}
