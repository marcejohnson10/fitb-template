select 
{{env_var('DBT_MY_DB')}} x_env_my_db,
{{env_var('DBT_MY_ENV')}} x_env_my_env,
'{{ target_model }}'  x_model,
'{{ target.type }}' x_type,
'{{ target.schema }}' x_schema,
'{{ sch }}' as x_sch,
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
'{{ this.name }}' as x_this_name
--'{{ this.identifier }}' as x_this_identifier

from dual