{#
  clean_whitespace(column)

  Trims leading/trailing spaces and collapses internal multiple spaces
  into a single space. Useful for cleaning host names, listing names, etc.

  Usage:
    {{ clean_whitespace('host_name') }} AS host_name
#}
{% macro clean_whitespace(column) %}
    TRIM(REGEXP_REPLACE({{ column }}, '\\s+', ' '))
{% endmacro %}
