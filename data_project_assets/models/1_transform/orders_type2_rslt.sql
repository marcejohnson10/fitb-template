{{
    config(
        materialized ='incremental',
        unique_key ='I_ORDER_ID',
        on_schema_change='sync_all_columns',
        change_data_capture_type = '2',
        match_conditions = []
    )
}}
with src as (
select {{ dbt_utils.star ( from=ref('orders_rslt') ) }},
       {{ get_cdc_metadata_columns('2') }}
from {{ ref('orders_rslt') }} 
where INGESTION_UNIQUE_KEY IN (
        SELECT INGESTION_UNIQUE_KEY
        FROM {{ create_stream (ref('orders_rslt')) }}
        where METADATA$ACTION = 'INSERT')
)
