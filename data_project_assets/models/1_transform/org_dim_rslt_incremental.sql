{{
    config(
        materialized="incremental",
        unique_key="cont_id",
        on_schema_change="sync_all_columns",
        incremental_strategy="merge",
    )
}}

select * from {{ ref("org_dim_rslt") }}
