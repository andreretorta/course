{#
  safe_divide(numerator, denominator)

  Divides two expressions avoiding division-by-zero errors.
  Returns NULL when the denominator is 0 or NULL.

  Usage:
    {{ safe_divide('total_revenue', 'total_nights') }}
#}
{% macro safe_divide(numerator, denominator) %}
    CASE
        WHEN {{ denominator }} IS NULL OR {{ denominator }} = 0
            THEN NULL
        ELSE {{ numerator }} / {{ denominator }}
    END
{% endmacro %}
