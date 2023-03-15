{% macro create_stream(tbl) %}


{%- if execute -%}
    {%- call statement('crt_stream', fetch_result=False) -%}
            {%- if flags.FULL_REFRESH  -%}
                CREATE OR REPLACE STREAM {{tbl}}_stream
                ON TABLE 
                {{tbl}} SHOW_INITIAL_ROWS = TRUE
            {%- else-%}
                CREATE STREAM IF NOT EXISTS {{tbl}}_stream
                ON TABLE 
                {{tbl}} SHOW_INITIAL_ROWS = TRUE
            {%- endif -%}   
    {%- endcall -%}
    {{tbl}}_STREAM
{%- endif -%}
{% endmacro %}