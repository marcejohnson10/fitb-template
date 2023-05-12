
{% macro marc_query_comment() %}

 {{model.name}}
{{model.alias}}
{{model.package_name}}
{{model.original_file_path}}
{{model.database}}
{{model.schema}}
{{model.unique_id}}
{{model.resource_type}}
{{model.tags}}

{% endmacro %}