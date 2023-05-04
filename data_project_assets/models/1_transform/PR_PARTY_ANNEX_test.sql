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
),
LEGAL_NM as (
SELECT CONT_ID,
CASE WHEN (ORGANIZATION_NAME IS NOT NULL ) THEN 1 ELSE 0 END AS Legal_Nm_Populated,
len(ORGANIZATION_NAME) AS Legal_Nm_Len,
POSITION(' ' IN ORGANIZATION_NAME) AS Legal_Nm_Space_Position
FROM src_stream 
WHERE NAME_USAGE_TYPE=1000003 and Z_CURRENT_FLAG='Y'
),
LEGAL_FULL_NM as (
SELECT CONT_ID,
CASE WHEN (ORGANIZATION_NAME IS NOT NULL ) THEN 1 ELSE 0 END AS Legal_Full_Nm_Populated,
len(ORGANIZATION_NAME) AS Legal_Full_Nm_Len,
POSITION(' ' IN ORGANIZATION_NAME) AS Legal_Full_Nm_Space_Position
FROM src_stream
WHERE NAME_USAGE_TYPE=1000004 and Z_CURRENT_FLAG='Y'    
)

select 
nvl(s1.cont_id,s2.cont_id) as cont_id, 
LEGAL_NM_POPULATED,
LEGAL_NM_LEN,
LEGAL_NM_SPACE_POSITION,
LEGAL_FULL_NM_POPULATED,
LEGAL_FULL_NM_LEN,
LEGAL_FULL_NM_SPACE_POSITION
from  LEGAL_NM s1 full outer join LEGAL_FULL_NM s2
on s1.cont_id = s2.cont_id


{{ transform_load_task(ref('PDP_ORG_NM_DIM')) }}
