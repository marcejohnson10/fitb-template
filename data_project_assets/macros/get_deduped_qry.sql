{#-- Macro to get deduplication query on source data -- #}
{%- macro get_deduped_qry (source)-%}

    {%- set unique_key = config.get('unique_key', []) -%}

    {%- if unique_key is sequence and unique_key is not mapping and unique_key is not string -%}
        {%- set unique_key = unique_key| join(', ') -%}
    {%- endif -%}
    

    {{ dbt_utils.deduplicate(
        relation=source ,
        partition_by=unique_key,
        order_by='INGESTION_TIMESTAMP desc',  
    )}}
    
{%- endmacro -%}