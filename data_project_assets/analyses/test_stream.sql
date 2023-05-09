-- Returns a list of the columns from a relation, so you can then iterate in a for loop
--adapter.get_columns_in_relation, dbt_utils.get_filtered_columns_in_relation
--{% set column_names = dbt_utils.get_filtered_columns_in_relation(from=ref('PDP_ORG_NM_DIM')) %}


{% set column_names = adapter.get_columns_in_relation('PDP_ORG_NM_DIM') %}
{{ column_names }}) 
{% set my_yaml_string = toyaml(column_names) %}
{{ my_yaml_string }}

{% call statement('desc_tbl', fetch_result=True) %}
				desc table DBT_DEMO_PROJECT.TRANSFORM.PDP_ORG_NM_DIM	
			{%- endcall-%}
			{%- set d_tbl = load_result('desc_tbl')['data'] -%}
            {{d_tbl}}
{% set my_yaml = toyaml(d_tbl) %}
{{ my_yaml }}