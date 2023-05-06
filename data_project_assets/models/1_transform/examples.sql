{% set mylist = ['name_usage_typ','Z_CURRENT_FLAG'] %}

with src as (
  select
  {% for i in mylist %}
     {{i}} as my_col 
    {% if not loop.last %} , {% endif %}
    {% endfor %}
    from {{ ref('PDP_ORG_NM_DIM') }}
   
)
select * from src