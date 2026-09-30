{#
  Default dbt behavior prefixes custom schemas with the target schema
  (e.g. "bronze_silver_amazon"). We want the custom schema to be the literal
  final schema name (silver_amazon, silver_enterprise, gold), so override it.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
