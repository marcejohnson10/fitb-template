{%- macro get_merge_sql(target, source, unique_key, dest_columns, predicates=none, called_by_tsk ='N') -%}

    {#-- Execute merge and task creation ddls based on use_sf_taks parameter --#}
    {%- set use_sf_task = config.get('use_sf_task', 'N') -%}
    {%- set change_data_capture_type = config.get('change_data_capture_type', '0') -%}
    
    {%- if change_data_capture_type not in ['1','2']-%}
        {{ adapter.dispatch('get_merge_sql')(target, source, unique_key, dest_columns, predicates) }}
    {%- else -%}
        {%- if ((called_by_tsk == 'Y' and use_sf_task == 'Y')  or (called_by_tsk =='N'  and use_sf_task == 'N') )-%} 
            {{ adapter.dispatch('get_merge_sql_mod')(target, source, unique_key, dest_columns, predicates, called_by_tsk) }}
        {%- elif use_sf_task == 'Y' and called_by_tsk =='N' -%}
            {{ return (fitb_dbt_utils.transform_load_task (target, called_by_merge = 'Y')) }}
        {%- endif -%} 
    {%- endif -%}
{%- endmacro -%}


{#-- Macro to Perform CDC 1 and 2 --#}
{%- macro snowflake__get_merge_sql_mod(target, source, unique_key, dest_columns, predicates, called_by_tsk) -%}
    {%- set change_data_capture_type = config.get('change_data_capture_type', 1) -%}
    {%- set use_sf_task = config.get('use_sf_task', 'N') -%}
    {%- set dest_col = [] -%}

    {#-- create source name and dest columns list based on called by task parameter --#}
    {%- if called_by_tsk == 'Y'-%}

        {%- set dest_col = dbt_utils.get_filtered_columns_in_relation(source) -%}

        {%- set source -%}
              (select * from {{source}}_STREAM where METADATA$ACTION = 'INSERT')
        {%- endset -%}
        {%- set source =  fitb_dbt_utils.get_deduped_qry (source) -%}

	    {%- if change_data_capture_type == '1' -%}
		    {%- do dest_col.append('Z_LAST_UPDATE_TIMESTAMP') -%}
	    {%-elif change_data_capture_type == '2' -%}
		    {%- do dest_col.append('Z_START_DATE') -%}
	    	{%- do dest_col.append('Z_END_DATE') -%}
	    	{%- do dest_col.append('Z_CURRENT_FLAG') -%}
	    {%- endif -%}
    {%- elif called_by_tsk == 'N'-%}
        {%- for col in dest_columns | map(attribute="name") -%}
            {%- do dest_col.append(col) -%}
        {%- endfor -%}
    {%- endif -%}


    {%- set predicates = [] if predicates is none else [] + predicates -%}   
    {%- set src_col_lst = [] -%}
    {%- set tgt_col_lst = [] -%}
    {%- set hash_col_src = [] -%}
    {%- set hash_col_tgt = [] -%}
    {#-- Target table columns --#}
    {%- set dest_cols_csv = get_quoted_csv(dest_col) -%}

    {#-- Variables user for CDC 1 --#}
    {%- set cdc_type_1_update_columns_strategy = config.get('cdc_type_1_update_columns_strategy', []) -%}   
    {%- set cdc_type_1_update_columns = config.get('cdc_type_1_update_columns', ['var_not_defined']) -%} 
    {%- set update_columns = [] -%}
        {#%- set merge_exclude_columns = config.get('merge_exclude_columns', [])  -%#}  
        {#%- set update_columns = dbt.get_merge_update_columns('cdc_type_1_update_columns', 'merge_exclude_columns', dest_columns) -%#}
    {%- set sql_header = config.get('sql_header', none) -%}

    {#-- Addition condition to be used while updating the records --#}
    {%- set match_conditions = [] -%}
    {%- set match_conditions = config.get('match_conditions') -%}
        {#%- set match_conditions = match_conditions|replace("src.","DBT_INTERNAL_SOURCE.") -%#}
        {#%- set match_conditions = match_conditions|replace("tgt.","DBT_INTERNAL_DEST.") -%#}

    {#-- Derive the unique key values --#}
    {%- if unique_key -%}
        {%- if unique_key is sequence and unique_key is not mapping and unique_key is not string -%}
            {%- for key in unique_key -%}
                {%- set this_key_match -%}
                    DBT_INTERNAL_SOURCE.{{ key }} = DBT_INTERNAL_DEST.{{ key }}
                {%- endset -%}
                {%- do predicates.append(this_key_match) -%}

                {%- set src_col -%}
                    DBT_INTERNAL_SOURCE.{{key}}
                {%- endset %}
                {%- do src_col_lst.append(src_col) -%}

                {%- set tgt_col -%}
                    DBT_INTERNAL_DEST.{{key}}
                {%- endset %}
                {%- do tgt_col_lst.append(tgt_col) -%} 

            {%- endfor -%}
        {%- else -%}
            {%- set unique_key_match -%}
                DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}
            {%- endset -%}
            {%- do predicates.append(unique_key_match) -%}

            {%- set src_col -%}
                DBT_INTERNAL_SOURCE.{{unique_key}}
            {%- endset %}
            {%- do src_col_lst.append(src_col) -%}

            {%- set tgt_col -%}
                DBT_INTERNAL_DEST.{{unique_key}}
            {%- endset %}            
            {%- do tgt_col_lst.append(tgt_col) -%}
        {%- endif -%}
    {%- else -%}
        {%- do predicates.append('FALSE') -%}
        {%- do src_col_lst.append('FALSE') -%}
        {%- do tgt_col_lst.append('FALSE') -%}
    {%- endif -%}

    {{ sql_header if sql_header is not none }}

    {#-- Perform CDC 1  --#}
    {%- if change_data_capture_type == '1' -%}
        
        {#--Derive the list of columns to be updated based on column strategy --#}
        {%- if cdc_type_1_update_columns_strategy =='exclude' -%}
            {%- if 'var_not_defined' in cdc_type_1_update_columns-%}
                {{ exceptions.raise_compiler_error(
                "Required config parameter 'update_columns' is missing in the model."
                )}}
            {%- elif cdc_type_1_update_columns == [] -%}
                 {{ exceptions.raise_compiler_error(
                "Model does not properly defines the required config parameter 'update_columns', expected the list of column names to be excluded.'"
                )}}
            {%- endif -%}
            {%- for column in dest_col-%}
                {%- if column not in cdc_type_1_update_columns-%}
                    {%-do update_columns.append(column) -%}
                {%- endif -%}
            {%- endfor -%}  
        {%- elif cdc_type_1_update_columns_strategy =='include' -%}
            {%- set update_columns = dest_col if config.get('cdc_type_1_update_columns', []) == [] else  config.get('cdc_type_1_update_columns') -%}         
            {%-if config.get('cdc_type_1_update_columns', []) != []  -%}
                {%- do update_columns.append('Z_LAST_UPDATE_TIMESTAMP') -%}  
            {%- endif -%}          
        {%- elif cdc_type_1_update_columns_strategy == [] -%}
            {{ exceptions.raise_compiler_error(
            "Required config parameter 'cdc_type_1_update_columns_strategy' is missing in the model."
            )}}
        {%- else -%} 
            {{ exceptions.raise_compiler_error(
            "Model does not properly defines the required config parameter 'cdc_type_1_update_columns_strategy', expected values - 'include' or 'exclude'."
            )}}
        {%- endif -%}   

         {#-- Create MERGE query to perform CDC 1 --#}
        merge into {{ target }} as DBT_INTERNAL_DEST
            using ({{ source }}) as DBT_INTERNAL_SOURCE
            on {{ predicates | join(' and ') }}
        {% if unique_key -%}
        when matched
            {%- if match_conditions -%}
                {%- for cond in match_conditions -%}
                    {%- set conds -%}
                        {{ cond }}
                    {%- endset -%}
                    {%- set conds = conds|replace("src.","DBT_INTERNAL_SOURCE.") -%}
                    {%- set conds = conds|replace("tgt.","DBT_INTERNAL_DEST.") -%}
                    {%- set conds = ' and '+ conds -%}
                        {{ conds }}
            {%- endfor -%}
            {% endif %} 
        and DBT_INTERNAL_SOURCE.INGESTION_UNIQUE_KEY <> DBT_INTERNAL_DEST.INGESTION_UNIQUE_KEY   
        then update set
        {%- for column_name in update_columns -%}
            {%- if column_name == 'Z_LAST_UPDATE_TIMESTAMP' %}
                {{ column_name }} = current_timestamp()
            {%- else %}
                {{ column_name }} = DBT_INTERNAL_SOURCE.{{ column_name }}
            {%- endif -%}  
            {%- if not loop.last-%}, {%- endif -%}
        {%- endfor -%}
        {% endif %}               
        when not matched then insert
            ({{ dest_cols_csv }})
        values
           ({%if called_by_tsk == 'Y' %}
                {%- for column_name in dest_col-%}
                    {%- if column_name == 'Z_LAST_UPDATE_TIMESTAMP' %}
                        current_timestamp()
                    {%- elif column_name == 'Z_START_DATE' %}
                    {%- else -%}
                        {{ column_name }}
                    {%- endif -%}
                    {%- if not loop.last-%}, {%- endif -%}
                {%- endfor -%}
            {%- elif called_by_tsk == 'N' -%}
                {{ dest_cols_csv }}
            {%- endif -%}
           );
    {% endif %}

    {#-- Perform CDC 2 --#}
    {%- if change_data_capture_type =='2' -%}

        {#-- Close the old records if new updates are made for a given unique key --#}
        ---Make previous records inactive for unique keys that are getting updated
        update {{target}} DBT_INTERNAL_DEST 
            set Z_END_DATE = current_date(),
                Z_CURRENT_FLAG = 'N'
        from (
        {{source}}
        ) as DBT_INTERNAL_SOURCE
        where {{ predicates | join(' and ') }}
        {% if match_conditions %}
                {%- for cond in match_conditions -%}
                    {%- set conds -%}
                        {{ cond }}
                    {%- endset -%}
                    {%- set conds = conds|replace("src.","DBT_INTERNAL_SOURCE.") -%}
                    {%- set conds = conds|replace("tgt.","DBT_INTERNAL_DEST.") -%}
                    {%- set conds = ' and '+ conds -%}
                        {{ conds }}
            {%- endfor -%}
        {%- endif -%}
        and DBT_INTERNAL_DEST.Z_CURRENT_FLAG = 'Y'
        and DBT_INTERNAL_SOURCE.INGESTION_UNIQUE_KEY <> DBT_INTERNAL_DEST.INGESTION_UNIQUE_KEY 
        ;
        {# -- Insert the new update from the source  #}
        ---Inserts the newly updated records 
        insert into {{target}}
        ({{ dest_cols_csv }})
        select 
        {% for column_name in dest_col %}
            {%- if column_name == 'Z_START_DATE' -%}
                current_date()
            {%- elif column_name == 'Z_END_DATE' -%}
                NULL
            {%- elif column_name == 'Z_CURRENT_FLAG' -%}
                'Y'
            {%- else -%}
                DBT_INTERNAL_SOURCE.{{ column_name }}
            {%- endif -%}
            {%- if not loop.last -%}, {% endif %}
        {%- endfor %}
        from (
            {{ source }}       
         )as DBT_INTERNAL_SOURCE, 
        (
            select * 
            from {{target}} as DBT_INTERNAL_DEST
            qualify row_number() over (partition by {{ tgt_col_lst | join(' , ') }}  order by DBT_INTERNAL_DEST.Z_END_DATE desc)  = 1 
        ) as DBT_INTERNAL_DEST
        where {{ predicates | join(' and ') }}
        {% if match_conditions %}
                {%- for cond in match_conditions -%}
                    {%- set conds -%}
                        {{ cond }}
                    {%- endset -%}
                    {%- set conds = conds|replace("src.","DBT_INTERNAL_SOURCE.") -%}
                    {%- set conds = conds|replace("tgt.","DBT_INTERNAL_DEST.") -%}
                    {%- set conds = ' and '+ conds -%}
                        {{ conds }}
            {%- endfor -%}
        {%- endif -%}
        and DBT_INTERNAL_SOURCE.INGESTION_UNIQUE_KEY <> DBT_INTERNAL_DEST.INGESTION_UNIQUE_KEY
        ;    
        {# -- Insert the new records from the source (new unique ids)  #}
        --- Inserts new unique id records
        insert into {{target}} 
        ({{ dest_cols_csv }})
        select 
        {% for column_name in dest_col %}
           {%- if column_name == 'Z_START_DATE' -%}
                current_date()
            {%- elif column_name == 'Z_END_DATE' -%}
                NULL
            {%- elif column_name == 'Z_CURRENT_FLAG' -%}
                'Y'
            {%- else -%}
                DBT_INTERNAL_SOURCE.{{ column_name }}
            {%- endif -%}
            {%- if not loop.last -%}, {% endif %}
        {%- endfor %}
        from ({{ source }}) as DBT_INTERNAL_SOURCE
        where ({{ src_col_lst | join(' , ') }}) not in
        (select distinct {{ tgt_col_lst | join(' , ') }} from {{target}} as DBT_INTERNAL_DEST ) 
		;
            
    {%- endif -%}
{%- endmacro -%}