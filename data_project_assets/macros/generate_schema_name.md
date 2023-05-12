{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- endif -%}

{#
    {% set dbt_job_run_id = env_var('DBT_CLOUD_JOB_ID') %} 
    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}
        {% if  env_var('DBT_ENVIRONMENT') == "sandbo" -%} 
            {{target.user}}_{{ custom_schema_name | trim }}
        {%- elif env_var('DBT_ENVIRONMENT') == "sandbox" -%}-%}
            {{default_schema}}_{{ custom_schema_name | trim }}
        {%- else -%}            
            {{ custom_schema_name | trim }}           
        {%- endif -%}
    {%- endif -%}
#}
{%- endmacro %}