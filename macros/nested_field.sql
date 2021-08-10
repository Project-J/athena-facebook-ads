{% macro nested_field(field, subfields) %}

    {{ adapter.dispatch('nested_field', 'facebook_ads')(field, subfields) }}

{% endmacro %}


{% macro athena__nested_field(field, subfields) %}

    {{field}}.{{ subfields|join('.') }}

{% endmacro %}
