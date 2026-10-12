{#
    Use the plain schema name from dbt_project.yml's +schema config
    (staging / curated) instead of dbt's default "<base>_<custom>"
    concatenation, so models land in the exact raw/staging/curated
    schemas pre-created by infra/postgres/init.sql.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
