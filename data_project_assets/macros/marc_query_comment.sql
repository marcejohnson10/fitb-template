
{% macro query_comment() %}

 {{node.name}}
{{node.alias}}
{{node.package_name}}
{{node.original_file_path}}
{{node.database}}
{{node.schema}}
{{node.unique_id}}
{{node.resource_type}}
{{node.tags}}

{% endmacro %}