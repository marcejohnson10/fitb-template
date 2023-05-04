{% macro root_task_get_stream(SRC_TBL_NM, TGT_TBL_NM , WH_NM, SCHDL) %}
    {{ log("starting  root_task_get_stream macro" ,  info=True) }}
    {#--generate audit task table name --#}
    {% if target.database == 'SANDBOX' %}
        {% set aud_tbl_nm ='SANDBOX_' + target.schema %}
    {% else %}
        {% set aud_tbl_nm = (target.database | replace('_DB', '') | replace('_NP', '') | replace('_P', '')) + '_' +  target.schema %}
    {% endif -%}
    


    {%- if execute -%}  
	        {%- if var("use_snowflake_tasks", true) -%}
                {%- call statement('sus_root_task', fetch_result=False) -%}
                    ALTER TASK IF EXISTS {{target.database}}.{{target.schema}}.{{SRC_TBL_NM}}_ROOT_LOAD_TASK SUSPEND
                {%- endcall -%}
                {{ log("suspended root task if exists " ,  info=True) }}
                
                {#--create root task --#}
                {%- call statement('crt_root_task', fetch_result=False) -%}
                    CREATE OR REPLACE TASK {{target.database}}.{{target.schema}}.{{SRC_TBL_NM}}_ROOT_LOAD_TASK
                    WAREHOUSE = {{WH_NM}} 
                    SCHEDULE = '{{SCHDL}}'
                    QUERY_TAG = '{{SRC_TBL_NM}}_ROOT_LOAD_TASK'
                    SUSPEND_TASK_AFTER_NUM_FAILURES = 1
                    COMMENT = 'This Root Task triggers child tasks whenever stream has new records'
                    WHEN
                    SYSTEM$STREAM_HAS_DATA('{{target.database}}.{{target.schema}}.{{TGT_TBL_NM}}_RAW_STREAM')
                    AS 
                    select 1
                {%- endcall -%}
                {{ log("created root task " ,  info=True) }}
                
                {#--create task audit table if not exists --#}
                {%- call statement('crt_aud_tbl', fetch_result=False) -%}
                    CREATE TABLE {{target.database}}.{{target.schema}}.{{aud_tbl_nm}}_TASK_AUDIT IF NOT EXISTS 
                    (T_DBT_PROJ varchar,
                    T_DB_NM varchar,
                    T_ROOT_TSK_NM varchar,
                    T_CHILD_TSK_NM varchar,
                    T_STRM_NM varchar,
                    T_VW_NM  varchar,
                    T_SRC_TBL_NM varchar,
                    T_TGT_TBL_NM varchar,
                    Z_STRT_TS timestamp_ltz,
                    Z_END_TS timestamp_ltz, 
                    T_STATUS varchar, 
                    T_ERR_MSG varchar)
                {%- endcall -%}  
                {{ log("created audit table ",  info=True) }} 

            {%- endif -%}
            {#--create stream --#}
            {{ fitb_dbt_utils.create_stream_on_json_table(SRC_TBL_NM, TGT_TBL_NM) }}

    {% endif %} 
    
    {% set ret  %}
        {{target.database}}.{{target.schema}}.{{TGT_TBL_NM}}_RAW_STREAM
    {% endset %}
    
    {{ log("returning stream name " ~ ret,  info=True) -}}
    {{ log("completed  root_task_get_stream macro" ,  info=True) }}

    {{ return (ret) }}

{% endmacro %}