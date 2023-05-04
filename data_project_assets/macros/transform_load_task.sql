{% macro transform_load_task(source, serverless = 'N', called_by_merge = 'N') %}
	

	{%- set use_sf_task = config.get('use_sf_task', 'N') -%}

	{#--dbt project name --#}
	{% set proj = "'" + project_name  + "'" %}

	{#--generate audit task table name --#}
	{% if target.database == 'SANDBOX' %}
		{% set aud_tbl_nm ='SANDBOX_' + target.schema %}
	{% else %}
		{% set aud_tbl_nm = (target.database | replace('_DB', '') | replace('_NP', '') | replace('_P', '')) + '_' +  target.schema %}
	{% endif -%}

	{%- if execute -%}	
		{%- if use_sf_task == 'Y'-%}
			{%- set unique_key = config.get('unique_key', []) -%} 
			
			{%- if called_by_merge == 'Y'-%}
				{#--get root task name --#}
                	    	{%- call statement('fully_qlfd_root_tsk_name', fetch_result=True) -%}
                    			select to_char(PREDECESSORS[0]) as task_nm 
                    			from table(information_schema.task_dependents(task_name => '{{this.database}}.{{this.schema}}.{{this.identifier}}_STREAM_TASK')) 
                    			where task_nm <> upper ('{{this.database}}.{{this.schema}}.{{this.identifier}}_STREAM_TASK');
                	     	{%- endcall-%}
                	     	{%- set fully_qlfd_root_tsk = load_result('fully_qlfd_root_tsk_name')['data'][0][0] -%}

                		--- Suspend Root Task : {{this.database}}.{{this.schema}}.{{root_tsk_nm}}
                		alter task IF EXISTS {{fully_qlfd_root_tsk}} suspend;
                		{%- call statement('task_ddl', fetch_result=True) -%}
                    			select get_ddl('task', '{{this.database}}.{{this.schema}}.{{source.identifier}}_STREAM_TASK',true) as ddl
                		{% endcall %}
                
                		--- Create Tranform load Task : {{source}}_STREAM_TASK
                		{{ load_result('task_ddl')['data'][0][0] }}

                		--- Resume above created tasks
                		alter task {{this}}_STREAM_TASK resume;
                		alter task {{fully_qlfd_root_tsk}}  resume;

			{%- else-%}
				{{ log("starting  transform_load_task macro" ,  info=True) }}

				{#--suspend root task --#}
				{%- call statement('susp_root_task', fetch_result=False) -%}
				alter task IF EXISTS {{this.database}}.{{this.schema}}.{{source.identifier}}_TRANSFORM_ROOT_LOAD_TASK suspend;
				{%- endcall -%}
				{{ log("suspended transform root task if exists " ,  info=True) }}
			
				{#
				-- Creates a child task that runs after the root task
				-- Inserts into Task Audit table and updates the status of the task
				-- Inserts from View to Target table
				#}
				{%- call statement('crt_child_task', fetch_result=False) -%}
					CREATE OR REPLACE TASK {{this}}_STREAM_TASK
						WAREHOUSE = {{target.warehouse}}
						QUERY_TAG = '{{ this.identifier }}_STREAM_TASK'
						COMMENT = 'this task loads table  {{ this }}'
						AFTER {{this.database}}.{{this.schema}}.{{source.identifier}}_TRANSFORM_ROOT_LOAD_TASK
						AS 
						EXECUTE IMMEDIATE
						$$
						begin
						-- Insert new record in the task audit table for current load 
						INSERT INTO
						{{target.database}}.{{target.schema}}.{{aud_tbl_nm}}_TASK_AUDIT values(
							upper({{proj}}),
							upper('{{target.database}}'),
							upper('{{source.identifier}}_TRANSFORM_ROOT_LOAD_TASK'),
							upper('{{this.identifier}}_STREAM_TASK'),
							upper('{{source.identifier}}_STREAM'),
							Null,
							upper('{{source.identifier}}'),
							upper('{{this.identifier}}'),
							current_timestamp(),
							null,
							'Child task completed, transform load task initiated',
							null
						); 
			
						begin transaction;
				-- Perform CDC operation
						{{ fitb_dbt_utils.get_merge_sql( this , source, unique_key, '', called_by_tsk = 'Y') }}
			
                        --  Update the audit table in case of successful execution  
						update {{target.database}}.{{target.schema}}.{{aud_tbl_nm}}_TASK_AUDIT a
								set a.T_STATUS =  'SUCCEEDED',
									a.Z_END_TS = current_timestamp()
								where a.Z_END_TS is null
								and   a.Z_STRT_TS = (select max(Z_STRT_TS) 
											from {{target.database}}.{{target.schema}}.{{aud_tbl_nm}}_TASK_AUDIT 
											where T_CHILD_TSK_NM =  '{{this.identifier}}_STREAM_TASK');
						
						commit;
			
						-- Updated the audit table in case of failure

								commit;
								
						end
						$$
				{%- endcall -%}
				{{ log("created transform load task" ,  info=True) }}
			
				{#--resume transform , child and root task --#}
				{%- call statement('resm_transform_task', fetch_result=False) -%}
					alter task if exists {{this}}_STREAM_TASK resume;
				{%- endcall -%}
				{{ log("resumed transform load task if exists " ,  info=True) }}
				
				{%- call statement('resm_root_transform_task', fetch_result=False) -%}
            		alter task if exists {{this.database}}.{{this.schema}}.{{source.identifier}}_TRANSFORM_ROOT_LOAD_TASK resume;
        		{%- endcall -%}
        		{{ log("resumed root transform load task if exists " ,  info=True) }}

			{{ log("completed transform_load_task macro" ,  info=True) }}               
			{%- endif -%}
		{%- endif -%}
	{%-endif-%}
{% endmacro %}