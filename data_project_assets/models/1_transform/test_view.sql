{{
    config(
        materialized ='view'
    )
}}

select * from {{ ref('PDP_ORG_NM_DIM') }}