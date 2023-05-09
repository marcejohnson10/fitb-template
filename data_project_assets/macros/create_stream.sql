{% macro create_stream(table) %}
	{%- if execute -%}
		{{ log("starting create_stream macro" ,  info=True) }}
		
		{%- set use_sf_task = config.get('use_sf_task', 'N') -%}
		
		   
    		{#--generate error integration name --#}
    		{% if '_P' in target.database %}
        		{% set err_int = 'aws_integration_snowflake_task_p' %}
    		{% else %}
    		{% set err_int = 'aws_integration_snowflake_task_np' %}
    		{% endif %}
		
		{% if use_sf_task == 'Y' %}

			{#--suspend root task --#}
			{%- call statement('susp_root_task', fetch_result=False) -%}
				alter task IF EXISTS {{this.database}}.{{this.schema}}.{{table.identifier}}_TRANSFORM_ROOT_LOAD_TASK suspend;
			{%- endcall -%}
			{{ log("suspended transform root task if exists " ,  info=True) }}

			{#--create root task for transform layer--#}
			{%- call statement('crt_root_task', fetch_result=False) -%}
				CREATE OR REPLACE TASK {{this.database}}.{{this.schema}}.{{table.identifier}}_TRANSFORM_ROOT_LOAD_TASK
				WAREHOUSE = {{target.warehouse}} 
				SCHEDULE = '1 minute'
				QUERY_TAG = '{{table.identifier}}_TRANSFORM_ROOT_LOAD_TASK'
			{#	ERROR_INTEGRATION = '{{err_int}}' #}
				SUSPEND_TASK_AFTER_NUM_FAILURES = 1
				COMMENT = 'This Root Task triggers child tasks in transform layer whenever stream has new records'
				WHEN
				SYSTEM$STREAM_HAS_DATA('{{table.database}}.{{table.schema}}.{{this.name}}_STREAM')
				AS 
				select 1
			{%- endcall -%}
			{{ log("created transform root task " ,  info=True) }}
			
		{%- else -%}
			{%- call statement('sus_root_task', fetch_result=False) -%}
				alter task IF EXISTS {{this.database}}.{{this.schema}}.{{table.identifier}}_TRANSFORM_ROOT_LOAD_TASK suspend;
			{%- endcall -%}
			{{ log("suspend transform root task if exists " ,  info=True) }}

			{%- call statement('drop_transform_task', fetch_result=False) -%}
            	drop task IF EXISTS {{this}}_STREAM_TASK ;
        	{%- endcall -%}
			{{ log("drop transform load task if exists " ,  info=True) }}

        	{%- call statement('drop_root_transform_task', fetch_result=False) -%}
           		drop task if exists {{this.database}}.{{this.schema}}.{{table.identifier}}_TRANSFORM_ROOT_LOAD_TASK;
        	{%- endcall -%}
			{{ log("drop transform root task if exists " ,  info=True) }}
		{%- endif -%}
	
			{%- call statement('crt_stream', fetch_result=False) -%}
					{%- if flags.FULL_REFRESH  -%}
						CREATE OR REPLACE STREAM {{table.database}}.{{table.schema}}.{{this.name}}_stream
						ON TABLE 
						{{table}} SHOW_INITIAL_ROWS = TRUE
						
						{{ log("re-created stream " ,  info=True) }}
					{%- else-%}
						CREATE STREAM IF NOT EXISTS {{table.database}}.{{table.schema}}.{{this.name}}_stream
						ON TABLE 
						{{table}} SHOW_INITIAL_ROWS = TRUE

						{{ log("created stream if not exists " ,  info=True) }}
					{%- endif -%}   
			{%- endcall -%}
			{{ log("returning stream name" ,  info=True) }}
			{{table.database}}.{{table.schema}}.{{this.name}}_STREAM
	
			{{ log("completed create_stream macro" ,  info=True) }}
		{%- endif -%}
{% endmacro %}