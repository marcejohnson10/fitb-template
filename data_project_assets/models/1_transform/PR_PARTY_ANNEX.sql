{{
    config(
        materialized ='incremental',
        unique_key = 'cont_id',
        on_schema_change='sync_all_columns',
        change_data_capture_type = '1',
        cdc_type_1_update_columns_strategy = 'include',
        cdc_type_1_update_columns = [],
        match_conditions = []
    )
}}

select 
nvl(s1.cont_id,s2.cont_id) as cont_id, 
nvl(s1.LEGAL_NM_POPULATED,t2.LEGAL_NM_POPULATED) LEGAL_NM_POPULATED,
nvl(s1.LEGAL_NM_LEN,t2.LEGAL_NM_LEN) LEGAL_NM_LEN,
nvl(s1.LEGAL_NM_SPACE_POSITION,t2.LEGAL_NM_SPACE_POSITION) LEGAL_NM_SPACE_POSITION,
nvl(s2.LEGAL_FULL_NM_POPULATED,t1.LEGAL_FULL_NM_POPULATED) LEGAL_FULL_NM_POPULATED,
nvl(s2.LEGAL_FULL_NM_LEN,t1.LEGAL_FULL_NM_LEN) LEGAL_FULL_NM_LEN,
nvl(s2.LEGAL_FULL_NM_SPACE_POSITION,t1.LEGAL_FULL_NM_SPACE_POSITION) LEGAL_FULL_NM_SPACE_POSITION,
       {{ get_cdc_metadata_columns('1') }}
from {{ ref('PR_ORG_LEGALNM') }} s1 full outer join {{ ref('PR_ORG_LEGALFULLNM') }} s2
on s1.cont_id = s2.cont_id
left outer join {{ this }} t1 
on s1.cont_id = t1.cont_id 
left outer join {{ this }} t2 
on s2.cont_id = t2.cont_id

