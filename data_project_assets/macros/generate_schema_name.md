{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}
        {% if  env_var('DBT_ENVIRONMENT') == "sandbox" -%} 
            {{target.user}}_{{ custom_schema_name | trim }}
        {%- else -%}            
            {{ custom_schema_name | trim }}           
        {%- endif -%}
    {%- endif -%}

{%- endmacro %}