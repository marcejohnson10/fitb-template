{#-- Macro to perform technical deduplication --#}
{#
--Input : 
--source  : eg. ref('<model>'), source ('<source>', '<table>') (mandatory)
--is_model : 'Y' if used in model and 'N' if used in child macro (mandatory)
--cte : CTE name in case if CTE is used in the model, by default take source name
#}
{%- macro do_tech_deduplicate(source, is_model, cte=source) -%}

    {#-- SQL query returned when macro used in the model --#}
    {%- if is_model == 'Y' -%}     
        select {{ fitb_dbt_utils.build_key_from_columns(source, ['INGESTION_TYPE', 'INGESTION_METADATA', 'INGESTION_TIMESTAMP']) }} AS INGESTION_UNIQUE_KEY, 
        *
        from {{ cte }}  
        qualify row_number () over (partition by INGESTION_UNIQUE_KEY order by INGESTION_TIMESTAMP)  = 1

    {#-- SQL query returned when macro used in child task macro --#}
    {%-elif  is_model == 'N'  -%}       
        with new_load as (
            select {{ fitb_dbt_utils.build_key_from_columns(source, ['INGESTION_TYPE', 'INGESTION_METADATA', 'INGESTION_TIMESTAMP']) }} AS INGESTION_UNIQUE_KEY,
            *
            from  {{ source }}
            qualify row_number () over (partition by INGESTION_UNIQUE_KEY order by INGESTION_TIMESTAMP)  = 1
        )
        select * from new_load
        where INGESTION_UNIQUE_KEY not in (select distinct INGESTION_UNIQUE_KEY from {{ this }})

    {%- endif -%}
{%- endmacro -%}
