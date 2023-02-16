    select *, current_timestamp as cur_timestamp from {{ ref('orders_raw') }} 
order by o_totalprice