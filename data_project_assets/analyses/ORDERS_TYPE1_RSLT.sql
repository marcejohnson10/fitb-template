{{
    config(
        enabled = false,
        materialized ='incremental',
        unique_key = 'INGESTION_UNIQUE_KEY',
        on_schema_change='sync_all_columns',
        change_data_capture_type = '1',
        cdc_type_1_update_columns_strategy = 'include',
        cdc_type_1_update_columns = ['T_ORDER_STATUS','T_ORDER_SUMMARY'],
        match_conditions = []
    )
}}

select src_1.INGESTION_UNIQUE_KEY,
tgt_1.INGESTION_TYPE,
	tgt_1.INGESTION_METADATA ,
	tgt_1.INGESTION_TIMESTAMP ,
	tgt_1.I_ORDER_ID,
	src_1.T_ORDER_STATUS ,
	tgt_1.T_ORDER_SUMMARY,
       {{ get_cdc_metadata_columns('1') }}
from {{ ref('orders_rslt') }} src_1 left outer join {{ this }} tgt_1 on src_1.INGESTION_UNIQUE_KEY = tgt_1.INGESTION_UNIQUE_KEY
where src_1.INGESTION_UNIQUE_KEY IN (
        SELECT INGESTION_UNIQUE_KEY
        FROM {{ create_stream (ref('orders_rslt')) }}
        where METADATA$ACTION = 'INSERT')
union all     
select src_1.INGESTION_UNIQUE_KEY,
tgt_1.INGESTION_TYPE,
	tgt_1.INGESTION_METADATA ,
	tgt_1.INGESTION_TIMESTAMP ,
	tgt_1.I_ORDER_ID,
	tgt_1.T_ORDER_STATUS ,
	src_1.T_ORDER_SUMMARY,
       {{ get_cdc_metadata_columns('1') }}
from {{ ref('orders_100_rslt') }} src_1 left outer join {{ this }} tgt_1 on src_1.INGESTION_UNIQUE_KEY = tgt_1.INGESTION_UNIQUE_KEY
where src_1.INGESTION_UNIQUE_KEY IN (
        SELECT INGESTION_UNIQUE_KEY
        FROM {{ create_stream (ref('orders_100_rslt')) }}
        where METADATA$ACTION = 'INSERT')   
