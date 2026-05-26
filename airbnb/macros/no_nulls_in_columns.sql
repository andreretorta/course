{% macro no_nulls_in_columns(model) %}
    SELECT *
    FROM {{ model }}
    WHERE
    {% for col in adapter.get_columns_in_relation(model) -%}
        {{ col.column }} IS NULL
        {%- if not loop.last %} OR {% endif %}
    {% endfor %}
{% endmacro %}
