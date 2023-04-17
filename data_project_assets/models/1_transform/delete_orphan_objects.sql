select * from (
{{find_orphan_tables_views('transform,raw', 'true')}}
)