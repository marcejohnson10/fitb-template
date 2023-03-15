{% macro get_cdc_metadata_col(change_data_capture_type) %}
    {#%- set change_data_capture_type = 'unexptd' if config.get('change_data_capture_type', 'missing') not in ['1','2','missing'] else config.get('change_data_capture_type')-%#}
    
    {#%- set change_data_capture_type = config.get('change_data_capture_type').str-%#}

    {%- set cdc1_stmt-%}
        current_timestamp()  as Z_LAST_UPDATE_TIMESTAMP
    {%- endset-%}

    {%- set cdc2_stmt-%}
        current_date() as Z_START_DATE,
        null  as Z_END_DATE,
        'Y' as Z_CURRENT_FLAG
    {%- endset-%}
    

    {%- if change_data_capture_type == '1' -%}
        {{ return(cdc1_stmt) }}
    {%- elif change_data_capture_type == '2' -%}
        {{ return(cdc2_stmt) }}
    {%- endif -%}

    {%- if  change_data_capture_type != '1' or change_data_capture_type != '2' -%}
        {{ exceptions.raise_compiler_error(
            "Missing or Incorrect input parameter, expected id '1' CDC type 1 and '2' for CDC type 2." 
        )}}
    {%- endif -%}

{% endmacro %}