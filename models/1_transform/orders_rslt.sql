{{
  config(
    materialized = 'incremental',
    unique_key = 'I_ORDER_ID',
    incremental_strategy = 'merge'
  )
}}

select * from {{ source('raw', 'orders') }}

    {% if is_incremental() %}
        where I_ORDER_STATUS = 'C'
    {% endif %}