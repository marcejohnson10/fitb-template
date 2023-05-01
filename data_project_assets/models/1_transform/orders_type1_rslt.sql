{{
    config(
        materialized ='incremental',
        unique_key ='I_ORDER_ID',
        on_schema_change='sync_all_columns',
        change_data_capture_type = '1',
        cdc_type_1_update_columns_strategy = 'include',
        cdc_type_1_update_columns = [],
        match_conditions = []
    )
}}

select {{ dbt_utils.star ( from=ref('orders_rslt') ) }},
       {{ get_cdc_metadata_columns('1') }}
from {{ ref('orders_rslt') }} 
where INGESTION_UNIQUE_KEY IN (
        SELECT INGESTION_UNIQUE_KEY
        FROM {{ create_stream (ref('orders_rslt')) }}
        where METADATA$ACTION = 'INSERT')

