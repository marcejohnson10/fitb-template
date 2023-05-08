{{
    config(
        materialized ='incremental',
        unique_key = 'cont_id',
        on_schema_change='sync_all_columns',
        change_data_capture_type = '1',
        cdc_type_1_update_columns_strategy = 'exclude',
        cdc_type_1_update_columns = ['cont_id'],
        match_conditions = [],
        use_sf_task = 'Y'
    )
}}

with src_stream as (
select {{ dbt_utils.star (from=ref('PDP_ORG_NM_DIM')) }} 
from {{ ref('PDP_ORG_NM_DIM') }}
where 1=0
union all
select {{ dbt_utils.star (from=ref('PDP_ORG_NM_DIM')) }} 
from {{ create_stream (ref('PDP_ORG_NM_DIM')) }}
where METADATA$ACTION = 'INSERT'     
)

{{ transform_load_task(ref('PDP_ORG_NM_DIM')) }}