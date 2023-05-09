{{ config(enabled = false, materialized='table') }}
select * from {{ ref('orders_rslt') }}