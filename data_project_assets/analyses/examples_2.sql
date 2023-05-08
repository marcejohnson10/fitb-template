
{% set mylist = ['name_usage_typ','Z_CURRENT_FLAG'] %}
{% set cols = dbtplyr.get_column_names( ref('PDP_ORG_NM_DIM') ) %}
{% set cols_n = dbtplyr.starts_with('n', cols) %}
{% set cols_ind = dbtplyr.starts_with('z', cols) %}
{% set cols_one_of = dbtplyr.one_of(['name_usage_type','Z_CURRENT_FLAG'], ref('PDP_ORG_NM_DIM')) %}
{% set cols_one_of_2 = dbtplyr.one_of(mylist, ref('PDP_ORG_NM_DIM')) %}

{# {% if execute %} #}
{%- set dest_col = dbt_utils.get_filtered_columns_in_relation(ref('PDP_ORG_NM_DIM')) -%}
{%- set metadata_col_lst = ['X_LOAD_TIMESTAMP', 'X_LOAD_TIMESTAMP','X_LAST_UPDATE_TIMESTAMP'] -%}
    {%- for metadata_col in metadata_col_lst-%}
        {%- set my_meta_list -%}
            XX_{{metadata_col}}
            {%- do dest_col.append(metadata_col) -%}
        {%- endset -%}
        {%- set my_meta_list_2 = dest_col.append(metadata_col) -%}
    {%- endfor-%}
{# {%- endif-%} #}
{#
{%- set tgt_col -%}
        DBT_INTERNAL_DEST.{{key}}
{%- endset %}
{%- do tgt_col_lst.append(tgt_col) -%} 
#}

  {% for i in my_list %}
    select {{i}} from {{ ref('PDP_ORG_NM_DIM') }}
   {% endfor %}

{{i}}
{{dest_col}}
{{my_meta_list_2}}
{{my_meta_list}}

{{mylist}}
{{cols_one_of}}
{{cols_one_of_2}}
  {{ dbtplyr.across(cols_n, "sum({{var}}) as {{var}}_tot") }}
  {{ dbtplyr.across(cols_ind, "mean({{var}}) as {{var}}_avg") }}

{#https://hub.getdbt.com/emilyriederer/dbtplyr/latest/#}
{#https://emilyriederer.github.io/dbtplyr/#!/macro/macro.dbtplyr.one_of#}