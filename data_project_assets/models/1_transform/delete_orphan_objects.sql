select 1 from {{ ref('orders') }}

{{find_orphan_tables_views('transform,raw', 'true')}}