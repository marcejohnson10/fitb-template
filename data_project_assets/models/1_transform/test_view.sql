{{
    config(
        materialized ='view',
        tags=["view"]

    )
}}

select * from {{ ref('PDP_ORG_NM_DIM') }} union all
select * from {{ create_stream (ref('PDP_ORG_NM_DIM')) }}