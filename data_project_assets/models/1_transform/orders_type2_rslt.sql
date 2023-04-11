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
select {{ dbt_utils.star ( from=ref('orders') ) }},
       {{ get_cdc_metadata_col('2') }}
from {{ ref('orders') }} 
where INGESTION_UNIQUE_KEY IN (
        SELECT INGESTION_UNIQUE_KEY
        FROM {{ create_stream (ref('orders')) }}
        where METADATA$ACTION = 'INSERT')
)
{{ get_deduped_qry ('src') }}