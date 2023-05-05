
{% set mylist = ['name_usage_typ','Z_CURRENT_FLAG'] %}
{% set cols = dbtplyr.get_column_names( ref('PDP_ORG_NM_DIM') ) %}
{% set cols_n = dbtplyr.starts_with('n', cols) %}
{% set cols_ind = dbtplyr.starts_with('z', cols) %}
{% set cols_one_of = dbtplyr.one_of(['name_usage_type','Z_CURRENT_FLAG'], ref('PDP_ORG_NM_DIM')) %}
{% set cols_one_of_2 = dbtplyr.one_of(mylist, ref('PDP_ORG_NM_DIM')) %}


{{mylist}}
{{cols_one_of}}
{{cols_one_of_2}}
  {{ dbtplyr.across(cols_n, "sum({{var}}) as {{var}}_tot") }}
  {{ dbtplyr.across(cols_ind, "mean({{var}}) as {{var}}_avg") }}

{#https://hub.getdbt.com/emilyriederer/dbtplyr/latest/#}
{#https://emilyriederer.github.io/dbtplyr/#!/macro/macro.dbtplyr.one_of#}