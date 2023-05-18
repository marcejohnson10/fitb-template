{{
    config(
        materialized ='incremental',
        unique_key = 'cont_id',
	incremental_strategy='merge',        
	on_schema_change='sync_all_columns'
    )
}}


select * from {{ ref('org_dim_rslt') }}