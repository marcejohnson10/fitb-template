{{ config(materialized='table') }}
{% set refs = [] %}
{%- set target_relation = this.incorporate(type='view') -%}
{%- set existing_relation = load_cached_relation(this) -%}

select distinct
'{{env_var('DBT_CLOUD_JOB_ID','x')}}' as env_var_DBT_CLOUD_JOB_ID,
'{{env_var('DBT_ENVIRONMENT')}}' as env_var_dbt_environments,
'{{model.package_name}}' as package_name,
'{{target_relation}}' as target_rel,
'{{existing_relation}}' as existing_rel,
'{{dbt_version}}' as dbt_version,
  {% for ref in model.refs %}  {%- do refs.append(ref[0]) -%} {% endfor %} '{{ tojson(refs) }}'::variant as refs,
'{{ model.dbt_schema_version }}' as v_dbt_schema_version,
'{{dbt_incremental_full_refresh}}' as x_dbt_incremental_full_refresh,
'{{ model.dbt_incremental_full_refresh }}' as v_dbt_incremental_full_refresh,
'{{ model.type }}' as v_type,
'{{ model.name }}' as v_name,
'{{ model.alias }}' as v_alias,
'{{ model.materialized }}' as v_materialized,
'{{ model.package_name }}' as v_package_name,
'{{ model.original_file_path }}' as v_original_file_path,
'{{ model.database }}' as v_database,
'{{ model.schema }}' as v_schema,
'{{ model.unique_id }}' as v_unique_id,
'{{ model.resource_type }}' as v_resource_type,
'{{ model.tags }}' as v_tags,
'{{model.name}}' as mod_nm,
'{{generate_schema_name()}}' as cust_schema,
{% if  env_var('DBT_ENVIRONMENT') == "sandbox" -%} 'sandbox' {%- else -%} 'x' {%- endif -%} as sandbox,
'{{env_var('DBT_DATABASE')}}' x_env_my_db,
'{{ var('proj_env') }}' as var_env,
'{{ var('proj_db') }}' as var_db,
'{{ var('proj_schema_transform') }}' as var_schema_tranform,
'{{ var('proj_schema_data_product') }}' as var_schema_data_product,
{#'{{ source('raw','orders')}}'  sr_database,#}
'{{custom_schema_name}}' as custom_sch_nm,
'{{ target_model }}'  x_model,
'{{ target.type }}' x_type,
'{{ target.schema }}' x_schema,
'{{ schema }}' y_schema,
'{{ target.name }}' x_name,
'{{ name }}' y_name,
'{{ target.database }}' x_database,
'{{ database }}' z_database,
'{{ target.profile_name }}' x_profile_name,
'{{ target.warehouse }}' x_warehouse,
'{{ target.user }}' x_user,
'{{ target.role }}' x_role,
'{{ target.account }}' x_account,
'{{ this }}' x_this,
'{{ this.schema }}' as x_this_schema,
'{{ this.name }}' as x_this_name,
'{{ this.identifier }}' as xx_this_identifier
from {{ ref('PR_ORG_LEGALFULLNM_DP') }} a 


