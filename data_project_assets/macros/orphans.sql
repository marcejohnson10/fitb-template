{% macro find_orphan_tables_views(schemas, dry_run='true') %}

{% do log('Finding orphaned snowflake TABLES & VIEWS associated with deleted dbt models.', info=True) %}

{% if execute %}
  {% set current_models=[] %}
  {% for node in graph.nodes.values()
     | selectattr("resource_type", "in", ["model"])%}
    {% do current_models.append(node.name) %}
  {% endfor %}
{% endif %}

{% set schemas = schemas.split(',') %}
{% set schemas = "'"+ modules.re.sub( '[ ]+', '', ('\',\''.join(schemas))) | upper +"'" %}

{% set cleanup_query %}
      with models_to_drop as (
        select
        table_catalog database_name, 
        table_schema schema_name, 
        table_name object_name,
          case 
            when table_type = 'BASE TABLE' then 'TABLE'
            when table_type = 'VIEW' then 'VIEW'
          end as relation_type,
          concat_ws('.', table_catalog, table_schema, table_name) as relation_name,
          '{{ target.name }}' as dbt_project_name,
          created, 
          last_altered,
          last_ddl,
          last_ddl_by,
          retention_time
        from 
          {{ target.database }}.information_schema.tables
        where table_schema in ({{schemas}}) --'{{ target.schema }}'
          and table_name not like '%_RAW'
          and table_name not like '%_TASK_AUDIT'
          and table_name not in
            ({%- for model in current_models -%}
                '{{ model.upper() }}'
                {%- if not loop.last -%}
                    ,
                {% endif %}
            {%- endfor -%}))
      
      select 
        'drop ' || relation_type || ' ' || relation_name || ';' as drop_commands,
        database_name, 
        schema_name, 
        object_name,
        relation_type,
        relation_name,
        dbt_project_name,
        created, 
        last_altered,
        last_ddl,
        last_ddl_by,
        retention_time
      from 
        models_to_drop
      where drop_commands is not null
      order by relation_type, relation_name
  {% endset %}

{{ return(cleanup_query) }}

{# {% do log(cleanup_query, info=True) %} #}

{% if execute %}
{% set drop_commands = run_query(cleanup_query).columns[0].values() %}
{% endif %}

{% if drop_commands %}
 {% do log('Perform these drop commands in Snowflake to clean orphaned snowflake TABLES & VIEWS associated with deleted dbt models.', info=True) %}
 {% do log('You will require correct snowflake privileges to perform drop on TABLES & VIEWS.', info=True) %} 
  {% for drop_command in drop_commands %}
    {% do log( '-- ' + drop_command, True) %}
    {% if dry_run == 'false' %}
      {% do run_query(drop_command) %}
    {% endif %}
  {% endfor %}

{% else %}
  {% do log('No orphan TABLES & VIEWS found.', True) %}
{% endif %}

{%- endmacro -%}