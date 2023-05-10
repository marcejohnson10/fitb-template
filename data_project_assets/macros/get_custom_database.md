{% macro generate_database_name(custom_database_name=none, node=none) -%}

    {%- set default_database = target.database -%}
    {%- if custom_database_name is none -%}

        {% if  env_var('DBT_ENVIRONMENT') == "sandbox" -%} env_var('DBT_DATABASE') 
            {% elif  env_var('DBT_MY_ENV') == "dev" -%} DBT_DEMO_PROJECT
            {% elif  env_var('DBT_MY_ENV') == "stage" -%} DBT_DEMO_PROJECT_STG
        {%- endif -%}

    {%- else -%}

        {{ custom_database_name | trim }}

    {%- endif -%}

{%- endmacro %}