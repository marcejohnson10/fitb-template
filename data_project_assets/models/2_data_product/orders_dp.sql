{{ config(materialized='table') }}
select * from {{ ref('orders_rslt') }}