---
title: "dbt Macros"
description: Reference for dbt macros developed by Snowplow
sidebar_position: 20
---

```mdx-code-block
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import ThemedImage from '@theme/ThemedImage';

export function DbtDetails(props) {
return <div className="dbt"><details>{props.children}</details></div>
}
```

:::caution

This page is auto-generated from our dbt packages, some information may be incomplete

:::
## Snowplow Utils
### App Id Filter {#macro.snowplow_utils.app_id_filter}

<DbtDetails><summary>
<code>macros/utils/app_id_filter.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro app_id_filter(app_ids) %}

  {%- if app_ids|length -%} 

    app_id in ('{{ app_ids|join("','") }}') --filter on app_id if provided

  {%- else -%}

    true

  {%- endif -%}

{% endmacro %}
```

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="model" label="Models" default>

- [model.snowplow_web.snowplow_web_base_events_this_run](#model.snowplow_web.snowplow_web_base_events_this_run)
- [model.snowplow_mobile.snowplow_mobile_base_events_this_run](#model.snowplow_mobile.snowplow_mobile_base_events_this_run)
- [model.snowplow_mobile.snowplow_mobile_base_sessions_lifecycle_manifest](#model.snowplow_mobile.snowplow_mobile_base_sessions_lifecycle_manifest)
- [model.snowplow_web.snowplow_web_base_sessions_lifecycle_manifest](#model.snowplow_web.snowplow_web_base_sessions_lifecycle_manifest)
- [model.snowplow_normalize.snowplow_normalize_base_events_this_run](#model.snowplow_normalize.snowplow_normalize_base_events_this_run)

</TabItem>
</Tabs>
</DbtDetails>

### Cast To Tstamp {#macro.snowplow_utils.cast_to_tstamp}

<DbtDetails><summary>
<code>macros/utils/cross_db/timestamp_functions.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro cast_to_tstamp(tstamp_literal) -%}
  {% if tstamp_literal is none or tstamp_literal|lower in ['null',''] %}
    cast(null as {{type_timestamp()}})
  {% else %}
    cast('{{tstamp_literal}}' as {{type_timestamp()}})
  {% endif %}
{%- endmacro %}
```

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="macros" label="Macros">

- [macro.snowplow_utils.get_run_limits](#macro.snowplow_utils.get_run_limits)
- [macro.snowplow_utils.return_base_new_event_limits](#macro.snowplow_utils.return_base_new_event_limits)
- [macro.snowplow_utils.get_session_lookback_limit](#macro.snowplow_utils.get_session_lookback_limit)
- [macro.snowplow_utils.return_limits_from_model](#macro.snowplow_utils.return_limits_from_model)

</TabItem>
</Tabs>
</DbtDetails>

### Coalesce Field Paths {#macro.snowplow_utils.coalesce_field_paths}

<DbtDetails><summary>
<code>macros/utils/bigquery/combine_column_versions/coalesce_field_paths.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro coalesce_field_paths(paths, field_alias, include_field_alias, relation_alias) %}
  
  {% set relation_alias = '' if relation_alias is none else relation_alias~'.' %}

  {% set field_alias = '' if not include_field_alias else ' as '~field_alias %}

  {% set joined_paths = relation_alias~paths|join(', '~relation_alias) %}

  {% set coalesced_field_paths = 'coalesce('~joined_paths~')'~field_alias %}

  {{ return(coalesced_field_paths) }}

{% endmacro %}
```

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="macros" label="Macros">

- [macro.snowplow_utils.combine_column_versions](#macro.snowplow_utils.combine_column_versions)

</TabItem>
</Tabs>
</DbtDetails>

### Combine Column Versions {#macro.snowplow_utils.combine_column_versions}

<DbtDetails><summary>
<code>macros/utils/bigquery/combine_column_versions/combine_column_versions.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro combine_column_versions(relation, column_prefix, required_fields=[], nested_level=none, level_filter='equalto', relation_alias=none, include_field_alias=true, array_index=0, max_nested_level=15, exclude_versions=[]) %}
  
  {# Create field_alias if not supplied i.e. is not tuple #}
  {% set required_fields_tmp = required_fields %}
  {% set required_fields = [] %}
  {% for field in required_fields_tmp %}
    {% set field_tuple = snowplow_utils.get_field_alias(field) %}
    {% do required_fields.append(field_tuple) %}
  {% endfor %}

  {% set required_field_names = required_fields|map(attribute=0)|list %}

  {# Determines correct level_limit. This limits recursive iterations during unnesting. #}
  {% set level_limit = snowplow_utils.get_level_limit(nested_level, level_filter, required_field_names) %}

  {# Limit level_limit to max_nested_level if required #}
  {% set level_limit = max_nested_level if level_limit is none or level_limit > max_nested_level else level_limit %}

  {%- set matched_columns = snowplow_utils.get_columns_in_relation_by_column_prefix(relation, column_prefix) -%}

  {# Removes excluded versions, assuming column name ends with a version of format 'X_X_X' #}
  {%- set filter_columns_by_version = snowplow_utils.exclude_column_versions(matched_columns, exclude_versions) -%}  

  {%- set flattened_fields_by_col_version = [] -%}

  {# Flatten fields within each column version. Returns nested arrays of dicts. #}
  {# Dict: {'field_name': str, 'field_alias': str, 'flattened_path': str, 'nested_level': int #}
  {% for column in filter_columns_by_version|sort(attribute='name', reverse=true) %}
    {% set flattened_fields = snowplow_utils.flatten_fields(fields=column.fields,
                                                            parent=column,
                                                            path=column.name,
                                                            array_index=array_index,
                                                            level_limit=level_limit
                                                            ) %}

    {% do flattened_fields_by_col_version.append(flattened_fields) %}

  {% endfor %}

  {# Flatten nested arrays and merges fields across col version. Returns array of dicts containing all field_paths for field. #}
  {# Dict: {'field_name': str, 'flattened_field_paths': str, 'nested_level': int #}
  {% set merged_fields = snowplow_utils.merge_fields_across_col_versions(flattened_fields_by_col_version) %}

  {# Filters merged_fields based on required_fields if provided, or the level filter if provided. Default return all fields. #}
  {% set matched_fields = snowplow_utils.get_matched_fields(fields=merged_fields,
                                                            required_field_names=required_field_names,
                                                            nested_level=nested_level,
                                                            level_filter=level_filter
                                                            ) %}

  {% set coalesced_field_paths = [] %}

  {% for field in matched_fields %}

    {% set passed_field_alias = required_fields|selectattr(0, "equalto", field.field_name)|map(attribute=1)|list %}
    {% set default_field_alias = field.field_name|replace('.', '_') %}
    {# Use passed_field_alias from required_fields if supplied #}
    {% set field_alias = default_field_alias if not passed_field_alias|length else passed_field_alias[0] %}

    {# Coalesce each field's path across all version of columns, ordered by latest col version. #}
    {% set coalesced_field_path = snowplow_utils.coalesce_field_paths(paths=field.field_paths,
                                                                      field_alias=field_alias,
                                                                      include_field_alias=include_field_alias,
                                                                      relation_alias=relation_alias) %}

    {% do coalesced_field_paths.append(coalesced_field_path) %}

  {% endfor %}

  {# Returns array of all coalesced field paths #}
  {{ return(coalesced_field_paths) }}

{% endmacro %}
```

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="macros" label="Macros">

- [macro.snowplow_utils.get_optional_fields](#macro.snowplow_utils.get_optional_fields)
- [macro.snowplow_normalize.bigquery__normalize_events](#macro.snowplow_normalize.bigquery__normalize_events)
- [macro.snowplow_mobile.bigquery__get_device_user_id_path_sql](#macro.snowplow_mobile.bigquery__get_device_user_id_path_sql)
- [macro.snowplow_normalize.bigquery__users_table](#macro.snowplow_normalize.bigquery__users_table)
- [macro.snowplow_mobile.bigquery__get_session_id_path_sql](#macro.snowplow_mobile.bigquery__get_session_id_path_sql)

</TabItem>
</Tabs>
</DbtDetails>

### Current Timestamp In Utc {#macro.snowplow_utils.current_timestamp_in_utc}

<DbtDetails><summary>
<code>macros/utils/cross_db/timestamp_functions.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

<Tabs groupId="dispatched_sql">
<TabItem value="raw" label="Raw" default>

```jinja2
{% macro current_timestamp_in_utc() -%}
  {{ return(adapter.dispatch('current_timestamp_in_utc', 'snowplow_utils')()) }}
{%- endmacro %}
```
</TabItem>
<TabItem value="default" label="default">

```jinja2
{% macro default__current_timestamp_in_utc() %}
    {{current_timestamp()}}
{% endmacro %}
```
</TabItem>
<TabItem value="snowflake" label="snowflake">

```jinja2
{% macro snowflake__current_timestamp_in_utc() %}
    convert_timezone('UTC', {{current_timestamp()}})::{{type_timestamp()}}
{% endmacro %}
```
</TabItem>
<TabItem value="redshift" label="redshift">

```jinja2
{% macro redshift__current_timestamp_in_utc() %}
    {{ return(snowplow_utils.default__current_timestamp_in_utc()) }}
{% endmacro %}
```
</TabItem>
<TabItem value="postgres" label="postgres">

```jinja2
{% macro postgres__current_timestamp_in_utc() %}
    (current_timestamp at time zone 'utc')::{{type_timestamp()}}
{% endmacro %}
```
</TabItem>
</Tabs>

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="model" label="Models" default>

- [model.snowplow_mobile.snowplow_mobile_sessions_this_run](#model.snowplow_mobile.snowplow_mobile_sessions_this_run)
- [model.snowplow_web.snowplow_web_users_this_run](#model.snowplow_web.snowplow_web_users_this_run)
- [model.snowplow_media_player.snowplow_media_player_media_stats](#model.snowplow_media_player.snowplow_media_player_media_stats)
- [model.snowplow_web.snowplow_web_page_views_this_run](#model.snowplow_web.snowplow_web_page_views_this_run)
- [model.snowplow_mobile.snowplow_mobile_screen_views_this_run](#model.snowplow_mobile.snowplow_mobile_screen_views_this_run)
- [model.snowplow_web.snowplow_web_sessions_this_run](#model.snowplow_web.snowplow_web_sessions_this_run)

</TabItem>
<TabItem value="macros" label="Macros">

- [macro.snowplow_utils.get_run_limits](#macro.snowplow_utils.get_run_limits)

</TabItem>
</Tabs>
</DbtDetails>

### Exclude Column Versions {#macro.snowplow_utils.exclude_column_versions}

<DbtDetails><summary>
<code>macros/utils/bigquery/combine_column_versions/exclude_column_versions.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro exclude_column_versions(columns, exclude_versions) %}
  {%- set filtered_columns_by_version = [] -%}  
  {% for column in columns %}
    {%- set col_version = column.name[-5:] -%}
    {% if col_version not in exclude_versions %}
      {% do filtered_columns_by_version.append(column) %}
    {% endif %}
  {% endfor %}

  {{ return(filtered_columns_by_version) }}

{% endmacro %}
```

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="macros" label="Macros">

- [macro.snowplow_utils.combine_column_versions](#macro.snowplow_utils.combine_column_versions)

</TabItem>
</Tabs>
</DbtDetails>

### Flatten Fields {#macro.snowplow_utils.flatten_fields}

<DbtDetails><summary>
<code>macros/utils/bigquery/combine_column_versions/flatten_fields.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro flatten_fields(fields, parent, path, array_index, level_limit=none, level_counter=1, flattened_fields=[], field_name='') %}
  
  {% for field in fields %}

    {# Only recurse up-until level_limit #}
    {% if level_limit is not none and level_counter > level_limit %}
      {{ return(flattened_fields) }}
    {% endif %}

    {# If parent column is an array then take element [array_index].  #}
    {% set delimiter = '[safe_offset(%s)].'|format(array_index) if parent.mode == 'REPEATED' else '.' %}
    {% set path = path~delimiter~field.name %}
    {% set field_name = field_name~'.'~field.name if field_name != '' else field_name~field.name %}

    {% set field_dict = {
                         'field_name': field_name,
                         'path': path,
                         'nested_level': level_counter
                         } %}

    {% do flattened_fields.append(field_dict) %}

    {# If field has nested fields recurse to extract all fields, unless array. #}
    {% if field.dtype == 'RECORD' and field.mode != 'REPEATED' %}

      {{ snowplow_utils.flatten_fields(
                                  fields=field.fields,
                                  parent=field,
                                  level_limit=level_limit,
                                  level_counter=level_counter+1,
                                  path=path,
                                  flattened_fields=flattened_fields,
                                  field_name=field_name
                                  ) }}

    {% endif %}

  {% endfor %}

  {{ return(flattened_fields) }}

{% endmacro %}
```

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="macros" label="Macros">

- [macro.snowplow_utils.combine_column_versions](#macro.snowplow_utils.combine_column_versions)
- [macro.snowplow_utils.flatten_fields](#macro.snowplow_utils.flatten_fields)

</TabItem>
</Tabs>
</DbtDetails>

### Get Cluster By {#macro.snowplow_utils.get_cluster_by}

<DbtDetails><summary>
<code>macros/utils/get_cluster_by.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro get_cluster_by(bigquery_cols=none, snowflake_cols=none) %}

  {% if target.type == 'bigquery' %}
    {{ return(bigquery_cols) }}
  {% elif target.type == 'snowflake' %}
    {{ return(snowflake_cols) }}
  {% endif %}
  
{% endmacro %}
```

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="model" label="Models" default>

- [model.snowplow_mobile.snowplow_mobile_users_aggs](#model.snowplow_mobile.snowplow_mobile_users_aggs)
- [model.snowplow_mobile.snowplow_mobile_sessions_sv_details](#model.snowplow_mobile.snowplow_mobile_sessions_sv_details)
- [model.snowplow_media_player.snowplow_media_player_base](#model.snowplow_media_player.snowplow_media_player_base)
- [model.snowplow_mobile.snowplow_mobile_sessions_aggs](#model.snowplow_mobile.snowplow_mobile_sessions_aggs)
- [model.snowplow_media_player.snowplow_media_player_media_stats](#model.snowplow_media_player.snowplow_media_player_media_stats)
- [model.snowplow_web.snowplow_web_users_aggs](#model.snowplow_web.snowplow_web_users_aggs)
- [model.snowplow_media_player.snowplow_media_player_base_this_run](#model.snowplow_media_player.snowplow_media_player_base_this_run)

</TabItem>
<TabItem value="macros" label="Macros">

- [macro.snowplow_mobile.default__mobile_cluster_by_fields_sessions_lifecycle](#macro.snowplow_mobile.default__mobile_cluster_by_fields_sessions_lifecycle)
- [macro.snowplow_web.default__web_cluster_by_fields_users](#macro.snowplow_web.default__web_cluster_by_fields_users)
- [macro.snowplow_web.default__web_cluster_by_fields_sessions_lifecycle](#macro.snowplow_web.default__web_cluster_by_fields_sessions_lifecycle)
- [macro.snowplow_mobile.default__cluster_by_fields_app_errors](#macro.snowplow_mobile.default__cluster_by_fields_app_errors)
- [macro.snowplow_mobile.default__mobile_cluster_by_fields_sessions](#macro.snowplow_mobile.default__mobile_cluster_by_fields_sessions)
- [macro.snowplow_mobile.default__mobile_cluster_by_fields_users](#macro.snowplow_mobile.default__mobile_cluster_by_fields_users)
- [macro.snowplow_web.default__web_cluster_by_fields_consent](#macro.snowplow_web.default__web_cluster_by_fields_consent)
- [macro.snowplow_web.default__web_cluster_by_fields_sessions](#macro.snowplow_web.default__web_cluster_by_fields_sessions)
- [macro.snowplow_web.default__web_cluster_by_fields_page_views](#macro.snowplow_web.default__web_cluster_by_fields_page_views)
- [macro.snowplow_mobile.default__mobile_cluster_by_fields_screen_views](#macro.snowplow_mobile.default__mobile_cluster_by_fields_screen_views)

</TabItem>
</Tabs>
</DbtDetails>

### Get Columns In Relation By Column Prefix {#macro.snowplow_utils.get_columns_in_relation_by_column_prefix}

<DbtDetails><summary>
<code>macros/utils/get_columns_in_relation_by_column_prefix.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro get_columns_in_relation_by_column_prefix(relation, column_prefix) %}

  {# Prevent introspective queries during parsing #}
  {%- if not execute -%}
    {{ return('') }}
  {% endif %}

  {%- set columns = adapter.get_columns_in_relation(relation) -%}
  
  {# get_columns_in_relation returns uppercase cols for snowflake so uppercase column_prefix #}
  {%- set column_prefix = column_prefix.upper() if target.type == 'snowflake' else column_prefix -%}

  {%- set matched_columns = [] -%}

  {# add columns with matching prefix to matched_columns #}
  {% for column in columns %}
    {% if column.name.startswith(column_prefix) %}
      {% do matched_columns.append(column) %}
    {% endif %}
  {% endfor %}

  {% if matched_columns|length %}
    {{ return(matched_columns) }}
  {% else %}
    {{ exceptions.raise_compiler_error("Snowplow: No columns found with prefix "~column_prefix) }}
  {% endif %}

{% endmacro %}
```

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="macros" label="Macros">

- [macro.snowplow_utils.combine_column_versions](#macro.snowplow_utils.combine_column_versions)

</TabItem>
</Tabs>
</DbtDetails>

### Get Enabled Snowplow Models {#macro.snowplow_utils.get_enabled_snowplow_models}

<DbtDetails><summary>
<code>macros/incremental_hooks/get_enabled_snowplow_models.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro get_enabled_snowplow_models(package_name, graph_object=none, models_to_run=var("models_to_run","")) -%}
  
  {# Override dbt graph object if graph_object is passed. Testing purposes #}
  {% if graph_object is not none %}
    {% set graph = graph_object %}
  {% endif %}
  
  {# models_to_run optionally passed using dbt ls command. This returns a string of models to be run. Split into list #}
  {% if models_to_run|length %}
    {% set selected_models = models_to_run.split(" ") %}
  {% else %}
    {% set selected_models = none %}
  {% endif %}

  {% set enabled_models = [] %}
  {% set untagged_snowplow_models = [] %}
  {% set snowplow_model_tag = package_name+'_incremental' %}
  {% set snowplow_events_this_run_path = 'model.'+package_name+'.'+package_name+'_base_events_this_run' %}

  {% if execute %}
    
    {% set nodes = graph.nodes.values() | selectattr("resource_type", "equalto", "model") %}
    
    {% for node in nodes %}
      {# If selected_models is specified, filter for these models #}
      {% if selected_models is none or node.name in selected_models %}

        {% if node.config.enabled and snowplow_model_tag not in node.tags and snowplow_events_this_run_path in node.depends_on.nodes %}

          {%- do untagged_snowplow_models.append(node.name) -%}

        {% endif %}

        {% if node.config.enabled and snowplow_model_tag in node.tags %}

          {%- do enabled_models.append(node.name) -%}

        {% endif %}

      {% endif %}
      
    {% endfor %}

    {% if untagged_snowplow_models|length %}
    {#
      Prints warning for models that reference snowplow_base_events_this_run but are untagged as 'snowplow_web_incremental'
      Without this tagging these models will not be inserted into the manifest, breaking the incremental logic.
      Only catches first degree dependencies rather than all downstream models
    #}
      {%- do exceptions.raise_compiler_error("Snowplow Warning: Untagged models referencing '"+package_name+"_base_events_this_run'. Please refer to the Snowplow docs on tagging. " 
      + "Models: "+ ', '.join(untagged_snowplow_models)) -%}
    
    {% endif %}

  {% endif %}

  {{ return(enabled_models) }}

{%- endmacro %}
```

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="model" label="Models" default>

- [model.snowplow_normalize.snowplow_normalize_base_new_event_limits](#model.snowplow_normalize.snowplow_normalize_base_new_event_limits)
- [model.snowplow_web.snowplow_web_base_new_event_limits](#model.snowplow_web.snowplow_web_base_new_event_limits)
- [model.snowplow_mobile.snowplow_mobile_base_new_event_limits](#model.snowplow_mobile.snowplow_mobile_base_new_event_limits)

</TabItem>
<TabItem value="macros" label="Macros">

- [macro.snowplow_utils.snowplow_incremental_post_hook](#macro.snowplow_utils.snowplow_incremental_post_hook)

</TabItem>
</Tabs>
</DbtDetails>

### Get Field Alias {#macro.snowplow_utils.get_field_alias}

<DbtDetails><summary>
<code>macros/utils/bigquery/combine_column_versions/get_field_alias.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro get_field_alias(field) %}
  
  {# Check if field is supplied as tuple e.g. (field_name, field_alias) #}
  {% if field is iterable and field is not string %}
    {{ return(field) }}
  {% else %}
    {{ return((field, field|replace('.', '_'))) }}
  {% endif %}
  
{% endmacro %}
```

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="macros" label="Macros">

- [macro.snowplow_utils.get_optional_fields](#macro.snowplow_utils.get_optional_fields)
- [macro.snowplow_utils.combine_column_versions](#macro.snowplow_utils.combine_column_versions)

</TabItem>
</Tabs>
</DbtDetails>

### Get Incremental Manifest Status {#macro.snowplow_utils.get_incremental_manifest_status}

<DbtDetails><summary>
<code>macros/incremental_hooks/get_incremental_manifest_status.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro get_incremental_manifest_status(incremental_manifest_table, models_in_run) -%}

  {% if not execute %}

    {{ return(['', '', '', '']) }}

  {% endif %}

  {% set last_success_query %}
    select min(last_success) as min_last_success,
           max(last_success) as max_last_success,
           coalesce(count(*), 0) as models
    from {{ incremental_manifest_table }}
    where model in ({{ snowplow_utils.print_list(models_in_run) }})
  {% endset %}

  {% set results = run_query(last_success_query) %}

  {% if execute %}

    {% set min_last_success = results.columns[0].values()[0] %}
    {% set max_last_success = results.columns[1].values()[0] %}
    {% set models_matched_from_manifest = results.columns[2].values()[0] %}
    {% set has_matched_all_models = true if models_matched_from_manifest == models_in_run|length else false %}

  {% endif %}

  {{ return([min_last_success, max_last_success, models_matched_from_manifest, has_matched_all_models]) }}

{%- endmacro %}
```

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="model" label="Models" default>

- [model.snowplow_normalize.snowplow_normalize_base_new_event_limits](#model.snowplow_normalize.snowplow_normalize_base_new_event_limits)
- [model.snowplow_web.snowplow_web_base_new_event_limits](#model.snowplow_web.snowplow_web_base_new_event_limits)
- [model.snowplow_mobile.snowplow_mobile_base_new_event_limits](#model.snowplow_mobile.snowplow_mobile_base_new_event_limits)

</TabItem>
</Tabs>
</DbtDetails>

### Get Incremental Manifest Table Relation {#macro.snowplow_utils.get_incremental_manifest_table_relation}

<DbtDetails><summary>
<code>macros/incremental_hooks/get_incremental_manifest_table_relation.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro get_incremental_manifest_table_relation(package_name) %}

  {%- set incremental_manifest_table = ref(package_name~'_incremental_manifest') -%}

  {{ return(incremental_manifest_table) }}

{% endmacro %}
```

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="macros" label="Macros">

- [macro.snowplow_utils.snowplow_incremental_post_hook](#macro.snowplow_utils.snowplow_incremental_post_hook)
- [macro.snowplow_utils.is_run_with_new_events](#macro.snowplow_utils.is_run_with_new_events)

</TabItem>
</Tabs>
</DbtDetails>

### Get Level Limit {#macro.snowplow_utils.get_level_limit}

<DbtDetails><summary>
<code>macros/utils/bigquery/combine_column_versions/get_level_limit.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro get_level_limit(level, level_filter, required_field_names) %}
  
  {% set accepted_level_filters = ['equalto','lessthan','greaterthan'] %}

  {% if level_filter is not in accepted_level_filters %}
    {% set incompatible_level_filter_error_message -%}
      Error: Incompatible level filter arg. Accepted args: {{accepted_level_filters|join(', ')}}
    {%- endset %}
    {{ return(snowplow_utils.throw_compiler_error(incompatible_level_filter_error_message)) }}
  {% endif %}

  {% if level is not none and required_field_names|length %}
    {% set double_filter_error_message -%}
      Error: Cannot filter fields by both `required_fields` and `level` arg. Please use only one.
    {%- endset %}
    {{ return(snowplow_utils.throw_compiler_error(double_filter_error_message)) }}
  {% endif %}

  {% if required_field_names|length and level_filter != 'equalto' %}
    {% set required_fields_error_message -%}
      Error: To filter fields using `required_fields` arg, `level_filter` must be set to `equalto`
    {%- endset %}
    {{ return(snowplow_utils.throw_compiler_error(required_fields_error_message)) }}
  {% endif %}

  {# level_limit is inclusive #}

  {% if level is not none %}

    {% if level_filter == 'equalto' %}

      {% set level_limit = level %}

    {% elif level_filter == 'lessthan' %}

      {% set level_limit = level -1  %}

    {% elif level_filter == 'greaterthan' %}

      {% set level_limit = none %}

    {% endif %}

  {% elif required_field_names|length %}

    {% set field_depths = [] %}
    {% for field in required_field_names %}
      {% set field_depth = field.split('.')|length %}
      {% do field_depths.append(field_depth) %}
    {% endfor %}

    {% set level_limit = field_depths|max %}

  {% else %}

    {# Case when selecting all available fields #}

    {% set level_limit = none %}

  {% endif %}

  {{ return(level_limit) }}

{% endmacro %}
```

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="macros" label="Macros">

- [macro.snowplow_utils.combine_column_versions](#macro.snowplow_utils.combine_column_versions)

</TabItem>
</Tabs>
</DbtDetails>

### Get Matched Fields {#macro.snowplow_utils.get_matched_fields}

<DbtDetails><summary>
<code>macros/utils/bigquery/combine_column_versions/get_matched_fields.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro get_matched_fields(fields, required_field_names, nested_level, level_filter) %}

  {% if not required_field_names|length %}

    {% if nested_level is none %}

      {% set matched_fields = fields %}

    {% else %}

      {% set matched_fields = fields|selectattr('nested_level',level_filter, nested_level)|list %}

    {% endif %}

  {% else %}

    {% set matched_fields = fields|selectattr('field_name','in', required_field_names)|list %}

  {% endif %}

  {{ return(matched_fields) }}

{% endmacro %}
```

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="macros" label="Macros">

- [macro.snowplow_utils.combine_column_versions](#macro.snowplow_utils.combine_column_versions)

</TabItem>
</Tabs>
</DbtDetails>

### Get New Event Limits Table Relation {#macro.snowplow_utils.get_new_event_limits_table_relation}

<DbtDetails><summary>
<code>macros/incremental_hooks/get_new_event_limits_table_relation.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro get_new_event_limits_table_relation(package_name) %}

  {%- set new_event_limits_table = ref(package_name~'_base_new_event_limits') -%}

  {{ return(new_event_limits_table) }}

{% endmacro %}
```

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="macros" label="Macros">

- [macro.snowplow_utils.is_run_with_new_events](#macro.snowplow_utils.is_run_with_new_events)

</TabItem>
</Tabs>
</DbtDetails>

### Get Optional Fields {#macro.snowplow_utils.get_optional_fields}

<DbtDetails><summary>
<code>macros/utils/bigquery/get_optional_fields.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro get_optional_fields(enabled, fields, col_prefix, relation, relation_alias, include_field_alias=true) -%}

  {%- if enabled -%}

    {%- set combined_fields = snowplow_utils.combine_column_versions(
                                    relation=relation,
                                    column_prefix=col_prefix,
                                    required_fields=fields|map(attribute='field')|list,
                                    relation_alias=relation_alias,
                                    include_field_alias=include_field_alias
                                    ) -%}

    {{ combined_fields|join(',\n') }}

  {%- else -%}

    {% for field in fields %}

      {%- set field_alias = snowplow_utils.get_field_alias(field.field)[1] -%}

      cast(null as {{ field.dtype }}) as {{ field_alias }} {%- if not loop.last %}, {% endif %}
    {% endfor %}

  {%- endif -%}

{% endmacro %}
```

</DbtDetails>

</DbtDetails>

### Get Partition By {#macro.snowplow_utils.get_partition_by}

<DbtDetails><summary>
<code>macros/utils/get_partition_by.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{%- macro get_partition_by(bigquery_partition_by=none, databricks_partition_by=none) -%}

  {% if target.type == 'bigquery' %}
    {{ return(bigquery_partition_by) }}
  {% elif target.type in ['databricks', 'spark'] %}
    {{ return(databricks_partition_by) }}
  {% endif %}

{%- endmacro -%}
```

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="model" label="Models" default>

- [model.snowplow_mobile.snowplow_mobile_users](#model.snowplow_mobile.snowplow_mobile_users)
- [model.snowplow_mobile.snowplow_mobile_users_aggs](#model.snowplow_mobile.snowplow_mobile_users_aggs)
- [model.snowplow_media_player.snowplow_media_player_base](#model.snowplow_media_player.snowplow_media_player_base)
- [model.snowplow_mobile.snowplow_mobile_user_mapping](#model.snowplow_mobile.snowplow_mobile_user_mapping)
- [model.snowplow_mobile.snowplow_mobile_sessions_aggs](#model.snowplow_mobile.snowplow_mobile_sessions_aggs)
- [model.snowplow_media_player.snowplow_media_player_media_stats](#model.snowplow_media_player.snowplow_media_player_media_stats)
- [model.snowplow_web.snowplow_web_users_aggs](#model.snowplow_web.snowplow_web_users_aggs)
- [model.snowplow_media_player.snowplow_media_player_base_this_run](#model.snowplow_media_player.snowplow_media_player_base_this_run)
- [model.snowplow_web.snowplow_web_sessions](#model.snowplow_web.snowplow_web_sessions)
- [model.snowplow_mobile.snowplow_mobile_screen_views](#model.snowplow_mobile.snowplow_mobile_screen_views)
- [model.snowplow_web.snowplow_web_user_mapping](#model.snowplow_web.snowplow_web_user_mapping)
- [model.snowplow_mobile.snowplow_mobile_sessions](#model.snowplow_mobile.snowplow_mobile_sessions)
- [model.snowplow_web.snowplow_web_base_sessions_lifecycle_manifest](#model.snowplow_web.snowplow_web_base_sessions_lifecycle_manifest)
- [model.snowplow_web.snowplow_web_page_views](#model.snowplow_web.snowplow_web_page_views)
- [model.snowplow_web.snowplow_web_users](#model.snowplow_web.snowplow_web_users)

</TabItem>
</Tabs>
</DbtDetails>

### Get Quarantine Sql {#macro.snowplow_utils.get_quarantine_sql}

<DbtDetails><summary>
<code>macros/incremental_hooks/quarantine_sessions.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro get_quarantine_sql(relation, max_session_length) %}

  {# Find sessions exceeding max_session_days #}
  {% set quarantine_sql -%}

    select
      session_id

    from {{ relation }}
    -- '=' since end_tstamp is restricted to start_tstamp + max_session_days
    where end_tstamp = {{ snowplow_utils.timestamp_add(
                              'day',
                              max_session_length,
                              'start_tstamp'
                              ) }}

  {%- endset %}

  {{ return(quarantine_sql) }}

{% endmacro %}
```

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="macros" label="Macros">

- [macro.snowplow_utils.default__quarantine_sessions](#macro.snowplow_utils.default__quarantine_sessions)
- [macro.snowplow_utils.postgres__quarantine_sessions](#macro.snowplow_utils.postgres__quarantine_sessions)

</TabItem>
</Tabs>
</DbtDetails>

### Get Run Limits {#macro.snowplow_utils.get_run_limits}

<DbtDetails><summary>
<code>macros/incremental_hooks/get_run_limits.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro get_run_limits(min_last_success, max_last_success, models_matched_from_manifest, has_matched_all_models, start_date) -%}

  {% set start_tstamp = snowplow_utils.cast_to_tstamp(start_date) %}
  {% set min_last_success = snowplow_utils.cast_to_tstamp(min_last_success) %}
  {% set max_last_success = snowplow_utils.cast_to_tstamp(max_last_success) %}

  {% if not execute %}
    {{ return('') }}
  {% endif %}

  {% if models_matched_from_manifest == 0 %}
    {# If no snowplow models are in the manifest, start from start_tstamp #}
    {% do snowplow_utils.log_message("Snowplow: No data in manifest. Processing data from start_date") %}

    {% set run_limits_query %}
      select {{start_tstamp}} as lower_limit,
             least({{ snowplow_utils.timestamp_add('day', var("snowplow__backfill_limit_days", 30), start_tstamp) }},
                   {{ snowplow_utils.current_timestamp_in_utc() }}) as upper_limit
    {% endset %}

  {% elif not has_matched_all_models %}
    {# If a new Snowplow model is added which isnt already in the manifest, replay all events up to upper_limit #}
    {% do snowplow_utils.log_message("Snowplow: New Snowplow incremental model. Backfilling") %}

    {% set run_limits_query %}
      select {{ start_tstamp }} as lower_limit,
             least({{ max_last_success }},
                   {{ snowplow_utils.timestamp_add('day', var("snowplow__backfill_limit_days", 30), start_tstamp) }}) as upper_limit
    {% endset %}

  {% elif min_last_success != max_last_success %}
    {# If all models in the run exists in the manifest but are out of sync, replay from the min last success to the max last success #}
    {% do snowplow_utils.log_message("Snowplow: Snowplow incremental models out of sync. Syncing") %}

    {% set run_limits_query %}
      select {{ snowplow_utils.timestamp_add('hour', -var("snowplow__lookback_window_hours", 6), min_last_success) }} as lower_limit,
             least({{ max_last_success }},
                  {{ snowplow_utils.timestamp_add('day', var("snowplow__backfill_limit_days", 30), min_last_success) }}) as upper_limit
    {% endset %}

  {% else %}
    {# Else standard run of the model #}
    {% do snowplow_utils.log_message("Snowplow: Standard incremental run") %}

    {% set run_limits_query %}
      select
        {{ snowplow_utils.timestamp_add('hour', -var("snowplow__lookback_window_hours", 6), min_last_success) }} as lower_limit,
        least({{ snowplow_utils.timestamp_add('day', var("snowplow__backfill_limit_days", 30), min_last_success) }},
              {{ snowplow_utils.current_timestamp_in_utc() }}) as upper_limit
    {% endset %}

  {% endif %}

  {{ return(run_limits_query) }}

{% endmacro %}
```

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="model" label="Models" default>

- [model.snowplow_normalize.snowplow_normalize_base_new_event_limits](#model.snowplow_normalize.snowplow_normalize_base_new_event_limits)
- [model.snowplow_web.snowplow_web_base_new_event_limits](#model.snowplow_web.snowplow_web_base_new_event_limits)
- [model.snowplow_mobile.snowplow_mobile_base_new_event_limits](#model.snowplow_mobile.snowplow_mobile_base_new_event_limits)

</TabItem>
</Tabs>
</DbtDetails>

### Get Schemas By Pattern {#macro.snowplow_utils.get_schemas_by_pattern}

<DbtDetails><summary>
<code>macros/utils/get_schemas_by_pattern.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

<Tabs groupId="dispatched_sql">
<TabItem value="raw" label="Raw" default>

```jinja2
{% macro get_schemas_by_pattern(schema_pattern, table_pattern) %}
    {{ return(adapter.dispatch('get_schemas_by_pattern', 'snowplow_utils')
        (schema_pattern, table_pattern)) }}
{% endmacro %}
```
</TabItem>
<TabItem value="default" label="default">

```jinja2
{% macro default__get_schemas_by_pattern(schema_pattern, table_pattern) %}
    {%- set schema_pattern= schema_pattern~'%' -%}

    {% set get_tables_sql = dbt_utils.get_tables_by_pattern_sql(schema_pattern, table_pattern='%') %}
    {% set results = [] if get_tables_sql.isspace() else run_query(get_tables_sql) %}
    {% set schemas = results|map(attribute='table_schema')|unique|list %}
    {{ return(schemas) }}

{% endmacro %}
```
</TabItem>
<TabItem value="databricks" label="databricks">

```jinja2
{% macro databricks__get_schemas_by_pattern(schema_pattern, table_pattern) %}
    {%- set schema_pattern= schema_pattern~'*' -%}

    {# Get all schemas with the target.schema prefix #}
    {%- set get_schemas_sql -%}
        SHOW SCHEMAS LIKE '{{schema_pattern}}';
    {%- endset -%}

    {% set results = run_query(get_schemas_sql) %}
    {% set schemas = results|map(attribute='databaseName')|unique|list %}

    {{ return(schemas) }}

{% endmacro %}
```
</TabItem>
<TabItem value="spark" label="spark">

```jinja2


{%- macro spark__get_schemas_by_pattern(schema_pattern, table_pattern) -%}
    {{ return(snowplow_utils.databricks__get_schemas_by_pattern(schema_pattern, table_pattern)) }}
{%- endmacro %}
```
</TabItem>
</Tabs>

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="macros" label="Macros">

- [macro.snowplow_utils.post_ci_cleanup](#macro.snowplow_utils.post_ci_cleanup)

</TabItem>
</Tabs>
</DbtDetails>

### Get Session Lookback Limit {#macro.snowplow_utils.get_session_lookback_limit}

<DbtDetails><summary>
<code>macros/incremental_hooks/get_session_lookback_limit.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro get_session_lookback_limit(lower_limit) %}
  
  {% if not execute %}
    {{ return('')}}
  {% endif %}

  {% set limit_query %}
    select
    {{ snowplow_utils.timestamp_add(
                'day', 
                -var("snowplow__session_lookback_days", 365),
                lower_limit) }} as session_lookback_limit

  {% endset %}

  {% set results = run_query(limit_query) %}
   
  {% if execute %}

    {% set session_lookback_limit = snowplow_utils.cast_to_tstamp(results.columns[0].values()[0]) %}

  {{ return(session_lookback_limit) }}

  {% endif %}

{% endmacro %}
```

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="model" label="Models" default>

- [model.snowplow_mobile.snowplow_mobile_base_sessions_lifecycle_manifest](#model.snowplow_mobile.snowplow_mobile_base_sessions_lifecycle_manifest)
- [model.snowplow_web.snowplow_web_base_sessions_lifecycle_manifest](#model.snowplow_web.snowplow_web_base_sessions_lifecycle_manifest)

</TabItem>
</Tabs>
</DbtDetails>

### Get Snowplow Delete Insert Sql {#macro.snowplow_utils.get_snowplow_delete_insert_sql}

<DbtDetails><summary>
<code>macros/materializations/snowplow_incremental/common/get_snowplow_delete_insert_sql.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro get_snowplow_delete_insert_sql(target, source, unique_key, dest_cols_csv, predicates) -%}
  {{ adapter.dispatch('get_snowplow_delete_insert_sql', 'snowplow_utils')(target, source, unique_key, dest_cols_csv, predicates) }}
{%- endmacro %}
```

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="macros" label="Macros">

- [macro.snowplow_utils.default__snowplow_delete_insert](#macro.snowplow_utils.default__snowplow_delete_insert)
- [macro.snowplow_utils.snowflake__snowplow_delete_insert](#macro.snowplow_utils.snowflake__snowplow_delete_insert)

</TabItem>
</Tabs>
</DbtDetails>

### Get Snowplow Merge Sql {#macro.snowplow_utils.get_snowplow_merge_sql}

<DbtDetails><summary>
<code>macros/materializations/snowplow_incremental/common/get_snowplow_merge_sql.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro get_snowplow_merge_sql(target, source, unique_key, dest_columns, predicates, include_sql_header) -%}
  {{ adapter.dispatch('get_snowplow_merge_sql', 'snowplow_utils')(target, source, unique_key, dest_columns, predicates, include_sql_header) }}
{%- endmacro %}
```

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="macros" label="Macros">

- [macro.snowplow_utils.snowflake__snowplow_merge](#macro.snowplow_utils.snowflake__snowplow_merge)
- [macro.snowplow_utils.default__snowplow_merge](#macro.snowplow_utils.default__snowplow_merge)
- [macro.snowplow_utils.databricks__snowplow_merge](#macro.snowplow_utils.databricks__snowplow_merge)

</TabItem>
</Tabs>
</DbtDetails>

### Get Snowplow Upsert Limits Sql {#macro.snowplow_utils.get_snowplow_upsert_limits_sql}

<DbtDetails><summary>
<code>macros/materializations/snowplow_incremental/common/get_snowplow_upsert_limits_sql.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro get_snowplow_upsert_limits_sql(tmp_relation, upsert_date_key, disable_upsert_lookback) -%}
  {{ adapter.dispatch('get_snowplow_upsert_limits_sql', 'snowplow_utils')(tmp_relation, upsert_date_key, disable_upsert_lookback) }}
{%- endmacro %}
```

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="macros" label="Macros">

- [macro.snowplow_utils.snowflake__snowplow_merge](#macro.snowplow_utils.snowflake__snowplow_merge)
- [macro.snowplow_utils.default__snowplow_merge](#macro.snowplow_utils.default__snowplow_merge)
- [macro.snowplow_utils.databricks__snowplow_merge](#macro.snowplow_utils.databricks__snowplow_merge)
- [macro.snowplow_utils.default__snowplow_delete_insert](#macro.snowplow_utils.default__snowplow_delete_insert)
- [macro.snowplow_utils.snowflake__snowplow_delete_insert](#macro.snowplow_utils.snowflake__snowplow_delete_insert)

</TabItem>
</Tabs>
</DbtDetails>

### Get Split To Array {#macro.snowplow_utils.get_split_to_array}

<DbtDetails><summary>
<code>macros/utils/cross_db/get_split_to_array.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

<Tabs groupId="dispatched_sql">
<TabItem value="raw" label="Raw" default>

```jinja2


{%- macro get_split_to_array(string_column, column_prefix) -%}
    {{ return(adapter.dispatch('get_split_to_array', 'snowplow_utils')(string_column, column_prefix)) }}
{%- endmacro -%}


```
</TabItem>
<TabItem value="default" label="default">

```jinja2
{% macro default__get_split_to_array(string_column, column_prefix) %}
   split({{column_prefix}}.{{string_column}}, ',')
{% endmacro %}
```
</TabItem>
<TabItem value="redshift" label="redshift">

```jinja2
{% macro redshift__get_split_to_array(string_column, column_prefix) %}
    split_to_array({{column_prefix}}.{{string_column}})
{% endmacro %}
```
</TabItem>
<TabItem value="postgres" label="postgres">

```jinja2
{% macro postgres__get_split_to_array(string_column, column_prefix) %}
    string_to_array({{column_prefix}}.{{string_column}}, ',')
{% endmacro %}
```
</TabItem>
</Tabs>

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="model" label="Models" default>

- [model.snowplow_media_player.snowplow_media_player_media_stats](#model.snowplow_media_player.snowplow_media_player_media_stats)

</TabItem>
</Tabs>
</DbtDetails>

### Get String Agg {#macro.snowplow_utils.get_string_agg}

<DbtDetails><summary>
<code>macros/utils/cross_db/get_string_agg.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

<Tabs groupId="dispatched_sql">
<TabItem value="raw" label="Raw" default>

```jinja2


{%- macro get_string_agg(string_column, column_prefix, separator=',', order_by_column=string_column, sort_numeric=false) -%}

  {{ return(adapter.dispatch('get_string_agg', 'snowplow_utils')(string_column, column_prefix, separator, order_by_column, sort_numeric)) }}

{%- endmacro -%}


```
</TabItem>
<TabItem value="default" label="default">

```jinja2
{% macro default__get_string_agg(string_column, column_prefix, separator=',', order_by_column=string_column, sort_numeric=false) %}

  {%- if sort_numeric -%}
    listagg({{column_prefix}}.{{string_column}}::varchar, '{{separator}}') within group (order by to_numeric({{column_prefix}}.{{order_by_column}}))

  {%- else %}
    listagg({{column_prefix}}.{{string_column}}, '{{separator}}') within group (order by {{column_prefix}}.{{order_by_column}})

  {%- endif -%}

{% endmacro %}
```
</TabItem>
<TabItem value="bigquery" label="bigquery">

```jinja2
{% macro bigquery__get_string_agg(string_column, column_prefix, separator=',', order_by_column=string_column, sort_numeric=false) %}

  {%- if sort_numeric -%}
    string_agg(cast({{column_prefix}}.{{string_column}} as string), '{{separator}}' order by cast({{column_prefix}}.{{order_by_column}} as numeric))

  {%- else %}
    string_agg(cast({{column_prefix}}.{{string_column}} as string), '{{separator}}' order by {{column_prefix}}.{{order_by_column}})

  {%- endif -%}

{% endmacro %}
```
</TabItem>
<TabItem value="databricks" label="databricks">

```jinja2
{% macro databricks__get_string_agg(string_column, column_prefix, separator=',', order_by_column=string_column, sort_numeric=false) %}

  {%- if sort_numeric -%}
    array_join(array_sort(collect_list(cast({{column_prefix}}.{{string_column}} as numeric))), '{{separator}}')

  {%- else %}
    array_join(array_sort(collect_list({{column_prefix}}.{{string_column}})), '{{separator}}')

  {%- endif -%}

{% endmacro %}
```
</TabItem>
<TabItem value="spark" label="spark">

```jinja2
{% macro spark__get_string_agg(string_column, column_prefix, separator=',', order_by_column=string_column, sort_numeric=false) %}

  {%- if sort_numeric -%}
    array_join(array_sort(collect_list(cast({{column_prefix}}.{{string_column}} as numeric))), '{{separator}}')

  {%- else %}
    array_join(array_sort(collect_list({{column_prefix}}.{{string_column}})), '{{separator}}')

  {%- endif -%}

{% endmacro %}
```
</TabItem>
<TabItem value="redshift" label="redshift">

```jinja2
{% macro redshift__get_string_agg(string_column, column_prefix, separator=',', order_by_column=string_column, sort_numeric=false) %}

  {%- if sort_numeric -%}
    listagg({{column_prefix}}.{{string_column}}::varchar, '{{separator}}') within group (order by text_to_numeric_alt({{column_prefix}}.{{order_by_column}}))

  {%- else %}
    listagg({{column_prefix}}.{{string_column}}::varchar, '{{separator}}') within group (order by {{column_prefix}}.{{order_by_column}})

  {%- endif -%}

{% endmacro %}
```
</TabItem>
<TabItem value="postgres" label="postgres">

```jinja2
{% macro postgres__get_string_agg(string_column, column_prefix, separator=',', order_by_column=string_column, sort_numeric=false) %}

  {%- if sort_numeric -%}
    string_agg({{column_prefix}}.{{string_column}}::varchar, '{{separator}}' order by {{column_prefix}}.{{order_by_column}}::decimal)

  {%- else %}
    string_agg({{column_prefix}}.{{string_column}}::varchar, '{{separator}}' order by {{column_prefix}}.{{order_by_column}})

  {%- endif -%}

{% endmacro %}
```
</TabItem>
</Tabs>

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="model" label="Models" default>

- [model.snowplow_media_player.snowplow_media_player_base_this_run](#model.snowplow_media_player.snowplow_media_player_base_this_run)

</TabItem>
</Tabs>
</DbtDetails>

### Get Successful Models {#macro.snowplow_utils.get_successful_models}

<DbtDetails><summary>
<code>macros/incremental_hooks/get_successful_models.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro get_successful_models(models=[], run_results=results) -%}

  {% set successful_models = [] %}
  {# Remove the patch version from dbt version #}
  {% set dbt_version_trunc = dbt_version.split('.')[0:2]|join('.')|float %}

  {% if execute %}

    {% for res in run_results -%}
      {# Filter for models #}
      {% if res.node.unique_id.startswith('model.') %}

        {% set is_model_to_include = true if not models|length or res.node.name in models else false %}

        {# run_results schema changed between dbt v0.18 and v0.19 so different methods to define success #}
        {% if dbt_version_trunc <= 0.18 %}
          {% set skipped = true if res.status is none and res.skip else false %}
          {% set errored = true if res.status == 'ERROR' else false %}
          {% set success = true if not (skipped or errored) else false %}
        {% else %}
          {% set success = true if res.status == 'success' else false %}
        {% endif %}

        {% if success and is_model_to_include %}

          {%- do successful_models.append(res.node.name) -%}

        {% endif %}

      {% endif %}

    {% endfor %}

    {{ return(successful_models) }}

  {% endif %}

{%- endmacro %}
```

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="macros" label="Macros">

- [macro.snowplow_utils.snowplow_incremental_post_hook](#macro.snowplow_utils.snowplow_incremental_post_hook)

</TabItem>
</Tabs>
</DbtDetails>

### Get Value By Target {#macro.snowplow_utils.get_value_by_target}

<DbtDetails><summary>
<code>macros/utils/get_value_by_target.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro get_value_by_target(dev_value, default_value, dev_target_name='dev') %}

  {% if target.name == dev_target_name %}
    {% set value = dev_value %}
  {% else %}
    {% set value = default_value %}
  {% endif %}

  {{ return(value) }}

{% endmacro %}
```

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="macros" label="Macros">

- [macro.snowplow_mobile.default__allow_refresh](#macro.snowplow_mobile.default__allow_refresh)
- [macro.snowplow_normalize.default__allow_refresh](#macro.snowplow_normalize.default__allow_refresh)
- [macro.snowplow_web.default__allow_refresh](#macro.snowplow_web.default__allow_refresh)

</TabItem>
</Tabs>
</DbtDetails>

### Is Run With New Events {#macro.snowplow_utils.is_run_with_new_events}

<DbtDetails><summary>
<code>macros/utils/is_run_with_new_events.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro is_run_with_new_events(package_name) %}

  {%- set new_event_limits_relation = snowplow_utils.get_new_event_limits_table_relation(package_name) -%}
  {%- set incremental_manifest_relation = snowplow_utils.get_incremental_manifest_table_relation(package_name) -%}

  {% if snowplow_utils.snowplow_is_incremental() %}

    {%- set node_identifier = this.identifier -%}
    {%- set base_sessions_lifecycle_identifier = package_name+'_base_sessions_lifecycle_manifest' -%}

    {# base_sessions_lifecycle not included in manifest so query directly. Otherwise use the manifest for performance #}
    {%- if node_identifier == base_sessions_lifecycle_identifier -%}
      {#Technically should be max(end_tstsamp) but table is partitioned on start_tstamp so cheaper to use.
        Worst case we update the manifest during a backfill when we dont need to, which should be v rare. #}
      {% set has_been_processed_query %}
        select 
          case when 
            (select upper_limit from {{ new_event_limits_relation }}) <= (select max(start_tstamp) from {{this}}) 
          then false 
        else true end
      {% endset %}

    {%- else -%}

      {% set has_been_processed_query %}
        select 
          case when 
            (select upper_limit from {{ new_event_limits_relation }}) 
            <= (select last_success from {{ incremental_manifest_relation }} where model = '{{node_identifier}}') 
          then false 
        else true end
      {% endset %}

    {%- endif -%}

    {% set results = run_query(has_been_processed_query) %}

    {% if execute %}
      {% set has_new_events = results.columns[0].values()[0] | as_bool() %}
      {# Snowflake: dbt 0.18 returns bools as ints. Ints are not accepted as predicates in Snowflake. Cast to be safe. #}
      {% set has_new_events = 'cast('~has_new_events~' as boolean)' %}
    {% endif %}

  {% else %}

    {% set has_new_events = true %}

  {% endif %}

  {{ return(has_new_events) }}

{% endmacro %}
```

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="model" label="Models" default>

- [model.snowplow_mobile.snowplow_mobile_users](#model.snowplow_mobile.snowplow_mobile_users)
- [model.snowplow_media_player.snowplow_media_player_base](#model.snowplow_media_player.snowplow_media_player_base)
- [model.snowplow_mobile.snowplow_mobile_user_mapping](#model.snowplow_mobile.snowplow_mobile_user_mapping)
- [model.snowplow_web.snowplow_web_sessions](#model.snowplow_web.snowplow_web_sessions)
- [model.snowplow_mobile.snowplow_mobile_screen_views](#model.snowplow_mobile.snowplow_mobile_screen_views)
- [model.snowplow_web.snowplow_web_user_mapping](#model.snowplow_web.snowplow_web_user_mapping)
- [model.snowplow_mobile.snowplow_mobile_sessions](#model.snowplow_mobile.snowplow_mobile_sessions)
- [model.snowplow_mobile.snowplow_mobile_base_sessions_lifecycle_manifest](#model.snowplow_mobile.snowplow_mobile_base_sessions_lifecycle_manifest)
- [model.snowplow_web.snowplow_web_base_sessions_lifecycle_manifest](#model.snowplow_web.snowplow_web_base_sessions_lifecycle_manifest)
- [model.snowplow_web.snowplow_web_page_views](#model.snowplow_web.snowplow_web_page_views)
- [model.snowplow_web.snowplow_web_users](#model.snowplow_web.snowplow_web_users)

</TabItem>
<TabItem value="macros" label="Macros">

- [macro.snowplow_normalize.databricks__normalize_events](#macro.snowplow_normalize.databricks__normalize_events)
- [macro.snowplow_normalize.snowflake__normalize_events](#macro.snowplow_normalize.snowflake__normalize_events)
- [macro.snowplow_normalize.snowflake__users_table](#macro.snowplow_normalize.snowflake__users_table)
- [macro.snowplow_normalize.bigquery__normalize_events](#macro.snowplow_normalize.bigquery__normalize_events)
- [macro.snowplow_normalize.databricks__users_table](#macro.snowplow_normalize.databricks__users_table)
- [macro.snowplow_normalize.bigquery__users_table](#macro.snowplow_normalize.bigquery__users_table)

</TabItem>
</Tabs>
</DbtDetails>

### Log Message {#macro.snowplow_utils.log_message}

<DbtDetails><summary>
<code>macros/utils/log_message.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

<Tabs groupId="dispatched_sql">
<TabItem value="raw" label="Raw" default>

```jinja2
{% macro log_message(message, is_printed=var('snowplow__has_log_enabled', true)) %}
    {{ return(adapter.dispatch('log_message', 'snowplow_utils')(message, is_printed)) }}
{% endmacro %}
```
</TabItem>
<TabItem value="default" label="default">

```jinja2
{% macro default__log_message(message, is_printed) %}
    {{ log(dbt_utils.pretty_log_format(message), info=is_printed) }}
{% endmacro %}
```
</TabItem>
</Tabs>

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="macros" label="Macros">

- [macro.snowplow_utils.get_run_limits](#macro.snowplow_utils.get_run_limits)
- [macro.snowplow_utils.snowplow_delete_from_manifest](#macro.snowplow_utils.snowplow_delete_from_manifest)
- [macro.snowplow_utils.print_run_limits](#macro.snowplow_utils.print_run_limits)

</TabItem>
</Tabs>
</DbtDetails>

### Materialization Snowplow Incremental Bigquery {#macro.snowplow_utils.materialization_snowplow_incremental_bigquery}

<DbtDetails><summary>
<code>macros/materializations/snowplow_incremental/bigquery/snowplow_incremental.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% materialization snowplow_incremental, adapter='bigquery' -%}

  {%- set full_refresh_mode = (should_full_refresh()) -%}

  {# Required keys. Throws error if not present #}
  {%- set unique_key = config.require('unique_key') -%}
  {%- set raw_partition_by = config.require('partition_by', none) -%}
  {%- set partition_by = adapter.parse_partition_by(raw_partition_by) -%}

  {# Raise error if dtype is int64. Unsupported. #}
  {% if partition_by.data_type == 'int64' %}
    {%- set wrong_dtype_message -%}
      Datatype int64 is not supported by 'snowplow_incremental'
      Please use one of the following: timestamp | date | datetime
    {%- endset -%}
    {% do exceptions.raise_compiler_error(wrong_dtype_message) %}
  {% endif %}

  {% set disable_upsert_lookback = config.get('disable_upsert_lookback') %}

  {%- set target_relation = this %}
  {%- set existing_relation = load_relation(this) %}
  {%- set tmp_relation = make_temp_relation(this) %}

  {# Validate early so we dont run SQL if the strategy is invalid or missing keys #}
  {% set strategy = snowplow_utils.snowplow_validate_get_incremental_strategy(config) -%}

  {%- set cluster_by = config.get('cluster_by', none) -%}

  {{ run_hooks(pre_hooks) }}

  {% if existing_relation is none %}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% elif existing_relation.is_view %}
      {#-- There's no way to atomically replace a view with a table on BQ --#}
      {{ adapter.drop_relation(existing_relation) }}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% elif full_refresh_mode %}
      {#-- If the partition/cluster config has changed, then we must drop and recreate --#}
      {% if not adapter.is_replaceable(existing_relation, partition_by, cluster_by) %}
          {% do log("Hard refreshing " ~ existing_relation ~ " because it is not replaceable") %}
          {{ adapter.drop_relation(existing_relation) }}
      {% endif %}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% else %}
      {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}

      {% set build_sql = snowplow_utils.snowplow_merge(
          tmp_relation,
          target_relation,
          unique_key,
          partition_by,
          dest_columns,
          disable_upsert_lookback) %}

  {% endif %}

  {%- call statement('main') -%}
    {{ build_sql }}
  {% endcall %}

  {{ run_hooks(post_hooks) }}

  {% set target_relation = this.incorporate(type='table') %}

  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
```

</DbtDetails>

</DbtDetails>

### Materialization Snowplow Incremental Databricks {#macro.snowplow_utils.materialization_snowplow_incremental_databricks}

<DbtDetails><summary>
<code>macros/materializations/snowplow_incremental/databricks/snowplow_incremental.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% materialization snowplow_incremental, adapter='databricks' -%}
  {%- set full_refresh_mode = (should_full_refresh()) -%}

  {# Required keys. Throws error if not present #}
  {%- set unique_key = config.require('unique_key') -%}
  {%- set upsert_date_key = config.require('upsert_date_key') -%}

  {% set disable_upsert_lookback = config.get('disable_upsert_lookback') %}

  {% set target_relation = this %}
  {% set existing_relation = load_relation(this) %}
  {% set tmp_relation = make_temp_relation(this) %}

  {# Validate early so we dont run SQL if the strategy is invalid or missing keys #}
  {% set strategy = snowplow_utils.snowplow_validate_get_incremental_strategy(config) -%}

  -- setup
  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% if existing_relation is none %}
    {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% elif existing_relation.is_view %}
    {#-- Can't overwrite a view with a table - we must drop --#}
    {{ log("Dropping relation " ~ target_relation ~ " because it is a view and this model is a table.") }}
    {% do adapter.drop_relation(existing_relation) %}
    {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% elif full_refresh_mode %}
    {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% else %}
    {% do run_query(create_table_as(True, tmp_relation, sql)) %}
    {% do adapter.expand_target_column_types(
           from_relation=tmp_relation,
           to_relation=target_relation) %}

    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}

    {% set build_sql = snowplow_utils.snowplow_merge( tmp_relation,
                                                      target_relation,
                                                      unique_key,
                                                      upsert_date_key,
                                                      dest_columns,
                                                      disable_upsert_lookback)%}
  {% endif %}

  {%- call statement('main') -%}
    {{ build_sql }}
  {%- endcall -%}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {% set target_relation = target_relation.incorporate(type='table') %}
  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
```

</DbtDetails>

</DbtDetails>

### Materialization Snowplow Incremental Default {#macro.snowplow_utils.materialization_snowplow_incremental_default}

<DbtDetails><summary>
<code>macros/materializations/snowplow_incremental/default/snowplow_incremental.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% materialization snowplow_incremental, default -%}

  {% set full_refresh_mode = flags.FULL_REFRESH %}

  {# Required keys. Throws error if not present #}
  {%- set unique_key = config.require('unique_key') -%}
  {%- set upsert_date_key = config.require('upsert_date_key') -%}
  
  {% set disable_upsert_lookback = config.get('disable_upsert_lookback') %}

  {% set target_relation = this %}
  {% set existing_relation = load_relation(this) %}
  {% set tmp_relation = make_temp_relation(this) %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set to_drop = [] %}
  {% if existing_relation is none %}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% elif existing_relation.is_view or full_refresh_mode %}
      {#-- Make sure the backup doesn't exist so we don't encounter issues with the rename below #}
      {% set backup_identifier = existing_relation.identifier ~ "__dbt_backup" %}
      {% set backup_relation = existing_relation.incorporate(path={"identifier": backup_identifier}) %}
      {% do adapter.drop_relation(backup_relation) %}

      {% do adapter.rename_relation(target_relation, backup_relation) %}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
      {% do to_drop.append(backup_relation) %}
  {% else %}
      {% set tmp_relation = make_temp_relation(target_relation) %}
      {% do run_query(create_table_as(True, tmp_relation, sql)) %}
      {% do adapter.expand_target_column_types(
             from_relation=tmp_relation,
             to_relation=target_relation) %}
      {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
      {% set build_sql = snowplow_utils.snowplow_delete_insert(
                                                     tmp_relation,
                                                     target_relation,
                                                     unique_key,
                                                     upsert_date_key,
                                                     dest_columns,
                                                     disable_upsert_lookback) %}
  {% endif %}

  {% call statement("main") %}
      {{ build_sql }}
  {% endcall %}

  {% if existing_relation is none or existing_relation.is_view or should_full_refresh() %} 
    {% do create_indexes(target_relation) %} 
  {% endif %} 

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {% do adapter.commit() %}

  {% for rel in to_drop %}
      {% do adapter.drop_relation(rel) %}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
```

</DbtDetails>

</DbtDetails>

### Materialization Snowplow Incremental Snowflake {#macro.snowplow_utils.materialization_snowplow_incremental_snowflake}

<DbtDetails><summary>
<code>macros/materializations/snowplow_incremental/snowflake/snowplow_incremental.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% materialization snowplow_incremental, adapter='snowflake' -%}

  {% set original_query_tag = set_query_tag() %}

  {%- set full_refresh_mode = (should_full_refresh()) -%}

  {# Required keys. Throws error if not present #}
  {%- set unique_key = config.require('unique_key') -%}
  {%- set upsert_date_key = config.require('upsert_date_key') -%}
  
  {% set disable_upsert_lookback = config.get('disable_upsert_lookback') %}

  {% set target_relation = this %}
  {% set existing_relation = load_relation(this) %}
  {% set tmp_relation = make_temp_relation(this) %}

  {# Validate early so we don't run SQL if the strategy is invalid or missing keys #}
  {% set strategy = snowplow_utils.snowplow_validate_get_incremental_strategy(config) -%}

  -- setup
  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% if existing_relation is none %}
    {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% elif existing_relation.is_view %}
    {#-- Can't overwrite a view with a table - we must drop --#}
    {{ log("Dropping relation " ~ target_relation ~ " because it is a view and this model is a table.") }}
    {% do adapter.drop_relation(existing_relation) %}
    {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% elif full_refresh_mode %}
    {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% else %}
    {% do run_query(create_table_as(True, tmp_relation, sql)) %}
    {% do adapter.expand_target_column_types(
           from_relation=tmp_relation,
           to_relation=target_relation) %}
    
    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}

    {% set build_sql = snowplow_utils.snowplow_snowflake_get_incremental_sql(strategy,
                                                                             tmp_relation,
                                                                             target_relation,
                                                                             unique_key,
                                                                             upsert_date_key,
                                                                             dest_columns,
                                                                             disable_upsert_lookback)%}
  {% endif %}

  {%- call statement('main') -%}
    {{ build_sql }}
  {%- endcall -%}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {% set target_relation = target_relation.incorporate(type='table') %}
  {% do persist_docs(target_relation, model) %}

  {% do unset_query_tag(original_query_tag) %}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
```

</DbtDetails>

</DbtDetails>

### Materialization Snowplow Incremental Spark {#macro.snowplow_utils.materialization_snowplow_incremental_spark}

<DbtDetails><summary>
<code>macros/materializations/snowplow_incremental/spark/snowplow_incremental.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% materialization snowplow_incremental, adapter='spark' -%}
  {%- set full_refresh_mode = (should_full_refresh()) -%}

  {# Required keys. Throws error if not present #}
  {%- set unique_key = config.require('unique_key') -%}
  {%- set upsert_date_key = config.require('upsert_date_key') -%}

  {% set disable_upsert_lookback = config.get('disable_upsert_lookback') %}

  {% set target_relation = this %}
  {% set existing_relation = load_relation(this) %}
  {% set tmp_relation = make_temp_relation(this) %}

  {# Validate early so we dont run SQL if the strategy is invalid or missing keys #}
  {% set strategy = snowplow_utils.snowplow_validate_get_incremental_strategy(config) -%}

  -- setup
  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% if existing_relation is none %}
    {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% elif existing_relation.is_view %}
    {#-- Can't overwrite a view with a table - we must drop --#}
    {{ log("Dropping relation " ~ target_relation ~ " because it is a view and this model is a table.") }}
    {% do adapter.drop_relation(existing_relation) %}
    {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% elif full_refresh_mode %}
    {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% else %}
    {% do run_query(create_table_as(True, tmp_relation, sql)) %}
    {% do adapter.expand_target_column_types(
           from_relation=tmp_relation,
           to_relation=target_relation) %}

    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}

    {% set build_sql = snowplow_utils.snowplow_merge( tmp_relation,
                                                      target_relation,
                                                      unique_key,
                                                      upsert_date_key,
                                                      dest_columns,
                                                      disable_upsert_lookback)%}
  {% endif %}

  {%- call statement('main') -%}
    {{ build_sql }}
  {%- endcall -%}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {% set target_relation = target_relation.incorporate(type='table') %}
  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
```

</DbtDetails>

</DbtDetails>

### Merge Fields Across Col Versions {#macro.snowplow_utils.merge_fields_across_col_versions}

<DbtDetails><summary>
<code>macros/utils/bigquery/combine_column_versions/merge_fields_across_col_versions.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro merge_fields_across_col_versions(fields_by_col_version) %}

  {# Flatten nested list of dicts into single list #}
  {% set all_cols = fields_by_col_version|sum(start=[]) %}

  {% set all_field_names = all_cols|map(attribute="field_name")|list %}

  {% set unique_field_names = all_field_names|unique|list %}

  {% set merged_fields = [] %}

  {% for field_name in unique_field_names %}

    {# Get all field_paths per field. Returned as array. #}
    {% set field_paths = all_cols|selectattr('field_name','equalto', field_name)|map(attribute='path')|list %}
    
    {# Get nested_level of field. Returned as single element array. #}
    {% set nested_level = all_cols|selectattr('field_name',"equalto", field_name)|map(attribute='nested_level')|list%}

    {% set merged_field = {
                           'field_name': field_name,
                           'field_paths': field_paths,
                           'nested_level': nested_level[0]
                           } %}

    {% do merged_fields.append(merged_field) %}

  {% endfor %}

  {{ return(merged_fields) }}

{% endmacro %}
```

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="macros" label="Macros">

- [macro.snowplow_utils.combine_column_versions](#macro.snowplow_utils.combine_column_versions)

</TabItem>
</Tabs>
</DbtDetails>

### N Timedeltas Ago {#macro.snowplow_utils.n_timedeltas_ago}

<DbtDetails><summary>
<code>macros/utils/n_timedeltas_ago.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro n_timedeltas_ago(n, timedelta_attribute) %}

  {% set arg_dict = {timedelta_attribute: n} %}
  {% set now = modules.datetime.datetime.now() %}
  {% set n_timedeltas_ago = (now - modules.datetime.timedelta(**arg_dict)) %}

  {{ return(n_timedeltas_ago) }}
  
{% endmacro %}
```

</DbtDetails>

</DbtDetails>

### Post Ci Cleanup {#macro.snowplow_utils.post_ci_cleanup}

<DbtDetails><summary>
<code>macros/utils/post_ci_cleanup.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro post_ci_cleanup(schema_pattern=target.schema) %}

  {# Get all schemas with the target.schema prefix #}
  {% set schemas = snowplow_utils.get_schemas_by_pattern(schema_pattern,table_pattern='%') %}

  {% if schemas|length %}

    {%- if target.type in ['databricks', 'spark'] -%}
      {# Generate sql to drop all identified schemas #}
      {% for schema in schemas -%}
        {%- set drop_schema_sql -%}
          DROP SCHEMA IF EXISTS {{schema}} CASCADE;
        {%- endset -%}

        {% do run_query(drop_schema_sql) %}

      {% endfor %}

    {%- else -%}
      {# Generate sql to drop all identified schemas #}
      {% set drop_schema_sql -%}

        {% for schema in schemas -%}
          DROP SCHEMA IF EXISTS {{schema}} CASCADE;
        {% endfor %}

      {%- endset %}

      {# Drop schemas #}
      {% do run_query(drop_schema_sql) %}

    {%- endif -%}

  {% endif %}

{% endmacro %}
```

</DbtDetails>

</DbtDetails>

### Print List {#macro.snowplow_utils.print_list}

<DbtDetails><summary>
<code>macros/utils/print_list.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro print_list(list) %}

  {%- for item in list %} '{{item}}' {%- if not loop.last %},{% endif %} {% endfor -%}

{% endmacro %}
```

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="macros" label="Macros">

- [macro.snowplow_utils.get_incremental_manifest_status](#macro.snowplow_utils.get_incremental_manifest_status)
- [macro.snowplow_utils.snowplow_delete_from_manifest](#macro.snowplow_utils.snowplow_delete_from_manifest)

</TabItem>
</Tabs>
</DbtDetails>

### Print Run Limits {#macro.snowplow_utils.print_run_limits}

<DbtDetails><summary>
<code>macros/incremental_hooks/get_incremental_manifest_status.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro print_run_limits(run_limits_relation) -%}

  {% set run_limits_query %}
    select lower_limit, upper_limit from {{ run_limits_relation }}
  {% endset %}

  {# Derive limits from manifest instead of selecting from limits table since run_query executes during 2nd parse the limits table is yet to be updated. #}
  {% set results = run_query(run_limits_query) %}

  {% if execute %}

    {% set lower_limit = snowplow_utils.tstamp_to_str(results.columns[0].values()[0]) %}
    {% set upper_limit = snowplow_utils.tstamp_to_str(results.columns[1].values()[0]) %}
    {% set run_limits_message = "Snowplow: Processing data between " + lower_limit + " and " + upper_limit %}

    {% do snowplow_utils.log_message(run_limits_message) %}

  {% endif %}

{%- endmacro %}
```

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="model" label="Models" default>

- [model.snowplow_normalize.snowplow_normalize_base_new_event_limits](#model.snowplow_normalize.snowplow_normalize_base_new_event_limits)
- [model.snowplow_web.snowplow_web_base_new_event_limits](#model.snowplow_web.snowplow_web_base_new_event_limits)
- [model.snowplow_mobile.snowplow_mobile_base_new_event_limits](#model.snowplow_mobile.snowplow_mobile_base_new_event_limits)

</TabItem>
</Tabs>
</DbtDetails>

### Quarantine Sessions {#macro.snowplow_utils.quarantine_sessions}

<DbtDetails><summary>
<code>macros/incremental_hooks/quarantine_sessions.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

<Tabs groupId="dispatched_sql">
<TabItem value="raw" label="Raw" default>

```jinja2
{% macro quarantine_sessions(package_name, max_session_length, src_relation=this) %}
  
  {{ return(adapter.dispatch('quarantine_sessions', 'snowplow_utils')(package_name, max_session_length, src_relation=this)) }}

{% endmacro %}
```
</TabItem>
<TabItem value="default" label="default">

```jinja2
{% macro default__quarantine_sessions(package_name, max_session_length, src_relation=this) %}
  
  {% set quarantined_sessions = ref(package_name~'_base_quarantined_sessions') %}
  
  {% set sessions_to_quarantine_sql = snowplow_utils.get_quarantine_sql(src_relation, max_session_length) %}

  merge into {{ quarantined_sessions }} trg
  using ({{ sessions_to_quarantine_sql }}) src
  on trg.session_id = src.session_id
  when not matched then insert (session_id) values(session_id);

{% endmacro %}
```
</TabItem>
<TabItem value="postgres" label="postgres">

```jinja2
{% macro postgres__quarantine_sessions(package_name, max_session_length, src_relation=this) %}
  
  {% set quarantined_sessions = ref(package_name~'_base_quarantined_sessions') %}
  {% set sessions_to_quarantine_tmp = 'sessions_to_quarantine_tmp' %}

  begin;

    create temporary table {{ sessions_to_quarantine_tmp }} as (
      {{ snowplow_utils.get_quarantine_sql(src_relation, max_session_length) }}
    );

    delete from {{ quarantined_sessions }}
    where session_id in (select session_id from {{ sessions_to_quarantine_tmp }});

    insert into {{ quarantined_sessions }} (
      select session_id from {{ sessions_to_quarantine_tmp }});

    drop table {{ sessions_to_quarantine_tmp }};

  commit;

{% endmacro %}
```
</TabItem>
</Tabs>

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="model" label="Models" default>

- [model.snowplow_web.snowplow_web_base_sessions_this_run](#model.snowplow_web.snowplow_web_base_sessions_this_run)

</TabItem>
</Tabs>
</DbtDetails>

### Return Base New Event Limits {#macro.snowplow_utils.return_base_new_event_limits}

<DbtDetails><summary>
<code>macros/incremental_hooks/return_base_new_event_limits.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro return_base_new_event_limits(base_events_this_run) -%}

  {% if not execute %}
    {{ return(['','',''])}}
  {% endif %}
  
  {% set limit_query %} 
    select 
      lower_limit, 
      upper_limit,
      {{ snowplow_utils.timestamp_add('day', 
                                     -var("snowplow__max_session_days", 3),
                                     'lower_limit') }} as session_start_limit

    from {{ base_events_this_run }} 
    {% endset %}

  {% set results = run_query(limit_query) %}
   
  {% if execute %}

    {% set lower_limit = snowplow_utils.cast_to_tstamp(results.columns[0].values()[0]) %}
    {% set upper_limit = snowplow_utils.cast_to_tstamp(results.columns[1].values()[0]) %}
    {% set session_start_limit = snowplow_utils.cast_to_tstamp(results.columns[2].values()[0]) %}

  {{ return([lower_limit, upper_limit, session_start_limit]) }}

  {% endif %}
{%- endmacro %}
```

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="model" label="Models" default>

- [model.snowplow_mobile.snowplow_mobile_base_sessions_this_run](#model.snowplow_mobile.snowplow_mobile_base_sessions_this_run)
- [model.snowplow_web.snowplow_web_base_sessions_this_run](#model.snowplow_web.snowplow_web_base_sessions_this_run)
- [model.snowplow_mobile.snowplow_mobile_base_sessions_lifecycle_manifest](#model.snowplow_mobile.snowplow_mobile_base_sessions_lifecycle_manifest)
- [model.snowplow_web.snowplow_web_base_sessions_lifecycle_manifest](#model.snowplow_web.snowplow_web_base_sessions_lifecycle_manifest)
- [model.snowplow_normalize.snowplow_normalize_base_events_this_run](#model.snowplow_normalize.snowplow_normalize_base_events_this_run)

</TabItem>
</Tabs>
</DbtDetails>

### Return Limits From Model {#macro.snowplow_utils.return_limits_from_model}

<DbtDetails><summary>
<code>macros/utils/return_limits_from_model.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro return_limits_from_model(model, lower_limit_col, upper_limit_col) -%}

  {% if not execute %}
    {{ return(['','']) }}
  {% endif %}
  
  {% set limit_query %} 
    select 
      min({{lower_limit_col}}) as lower_limit,
      max({{upper_limit_col}}) as upper_limit
    from {{ model }} 
    {% endset %}

  {% set results = run_query(limit_query) %}
   
  {% if execute %}

    {% set lower_limit = snowplow_utils.cast_to_tstamp(results.columns[0].values()[0]) %}
    {% set upper_limit = snowplow_utils.cast_to_tstamp(results.columns[1].values()[0]) %}

  {{ return([lower_limit, upper_limit]) }}

  {% endif %}
{%- endmacro %}
```

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="model" label="Models" default>

- [model.snowplow_web.snowplow_web_base_events_this_run](#model.snowplow_web.snowplow_web_base_events_this_run)
- [model.snowplow_mobile.snowplow_mobile_base_session_context](#model.snowplow_mobile.snowplow_mobile_base_session_context)
- [model.snowplow_mobile.snowplow_mobile_base_events_this_run](#model.snowplow_mobile.snowplow_mobile_base_events_this_run)

</TabItem>
</Tabs>
</DbtDetails>

### Set Query Tag {#macro.snowplow_utils.set_query_tag}

<DbtDetails><summary>
<code>macros/utils/set_query_tag.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

<Tabs groupId="dispatched_sql">
<TabItem value="raw" label="Raw" default>

```jinja2
{%- macro set_query_tag(statement) -%}
  {{ return(adapter.dispatch('set_query_tag', 'snowplow_utils')(statement)) }}
{%- endmacro -%}


```
</TabItem>
<TabItem value="default" label="default">

```jinja2
{% macro default__set_query_tag(statement) %}
    
{% endmacro %}
```
</TabItem>
<TabItem value="snowflake" label="snowflake">

```jinja2
{% macro snowflake__set_query_tag(statement) %}
    alter session set query_tag = '{{ statement }}';
{% endmacro %}
```
</TabItem>
</Tabs>

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="model" label="Models" default>

- [model.snowplow_mobile.snowplow_mobile_users](#model.snowplow_mobile.snowplow_mobile_users)
- [model.snowplow_mobile.snowplow_mobile_incremental_manifest](#model.snowplow_mobile.snowplow_mobile_incremental_manifest)
- [model.snowplow_web.snowplow_web_base_events_this_run](#model.snowplow_web.snowplow_web_base_events_this_run)
- [model.snowplow_web.snowplow_web_sessions_lasts](#model.snowplow_web.snowplow_web_sessions_lasts)
- [model.snowplow_web.snowplow_web_pv_scroll_depth](#model.snowplow_web.snowplow_web_pv_scroll_depth)
- [model.snowplow_mobile.snowplow_mobile_sessions_this_run](#model.snowplow_mobile.snowplow_mobile_sessions_this_run)
- [model.snowplow_mobile.snowplow_mobile_users_sessions_this_run](#model.snowplow_mobile.snowplow_mobile_users_sessions_this_run)
- [model.snowplow_web.snowplow_web_incremental_manifest](#model.snowplow_web.snowplow_web_incremental_manifest)
- [model.snowplow_web.snowplow_web_users_this_run](#model.snowplow_web.snowplow_web_users_this_run)
- [model.snowplow_mobile.snowplow_mobile_users_aggs](#model.snowplow_mobile.snowplow_mobile_users_aggs)
- [model.snowplow_mobile.snowplow_mobile_sessions_sv_details](#model.snowplow_mobile.snowplow_mobile_sessions_sv_details)
- [model.snowplow_mobile.snowplow_mobile_base_sessions_this_run](#model.snowplow_mobile.snowplow_mobile_base_sessions_this_run)
- [model.snowplow_media_player.snowplow_media_player_base](#model.snowplow_media_player.snowplow_media_player_base)
- [model.snowplow_web.snowplow_web_base_quarantined_sessions](#model.snowplow_web.snowplow_web_base_quarantined_sessions)
- [model.snowplow_mobile.snowplow_mobile_users_lasts](#model.snowplow_mobile.snowplow_mobile_users_lasts)
- [model.snowplow_web.snowplow_web_sessions_aggs](#model.snowplow_web.snowplow_web_sessions_aggs)
- [model.snowplow_mobile.snowplow_mobile_user_mapping](#model.snowplow_mobile.snowplow_mobile_user_mapping)
- [model.snowplow_web.snowplow_web_users_lasts](#model.snowplow_web.snowplow_web_users_lasts)
- [model.snowplow_web.snowplow_web_users_sessions_this_run](#model.snowplow_web.snowplow_web_users_sessions_this_run)
- [model.snowplow_mobile.snowplow_mobile_sessions_aggs](#model.snowplow_mobile.snowplow_mobile_sessions_aggs)
- [model.snowplow_media_player.snowplow_media_player_pivot_base](#model.snowplow_media_player.snowplow_media_player_pivot_base)
- [model.snowplow_media_player.snowplow_media_player_media_stats](#model.snowplow_media_player.snowplow_media_player_media_stats)
- [model.snowplow_normalize.snowplow_normalize_base_new_event_limits](#model.snowplow_normalize.snowplow_normalize_base_new_event_limits)
- [model.snowplow_web.snowplow_web_base_new_event_limits](#model.snowplow_web.snowplow_web_base_new_event_limits)
- [model.snowplow_web.snowplow_web_users_aggs](#model.snowplow_web.snowplow_web_users_aggs)
- [model.snowplow_media_player.snowplow_media_player_plays_by_pageview](#model.snowplow_media_player.snowplow_media_player_plays_by_pageview)
- [model.snowplow_media_player.snowplow_media_player_base_this_run](#model.snowplow_media_player.snowplow_media_player_base_this_run)
- [model.snowplow_web.snowplow_web_sessions](#model.snowplow_web.snowplow_web_sessions)
- [model.snowplow_web.snowplow_web_page_views_this_run](#model.snowplow_web.snowplow_web_page_views_this_run)
- [model.snowplow_mobile.snowplow_mobile_screen_views](#model.snowplow_mobile.snowplow_mobile_screen_views)
- [model.snowplow_web.snowplow_web_pv_engaged_time](#model.snowplow_web.snowplow_web_pv_engaged_time)
- [model.snowplow_web.snowplow_web_base_sessions_this_run](#model.snowplow_web.snowplow_web_base_sessions_this_run)
- [model.snowplow_web.snowplow_web_user_mapping](#model.snowplow_web.snowplow_web_user_mapping)
- [model.snowplow_mobile.snowplow_mobile_users_this_run](#model.snowplow_mobile.snowplow_mobile_users_this_run)
- [model.snowplow_mobile.snowplow_mobile_sessions](#model.snowplow_mobile.snowplow_mobile_sessions)
- [model.snowplow_web.snowplow_web_base_sessions_lifecycle_manifest](#model.snowplow_web.snowplow_web_base_sessions_lifecycle_manifest)
- [model.snowplow_mobile.snowplow_mobile_base_new_event_limits](#model.snowplow_mobile.snowplow_mobile_base_new_event_limits)
- [model.snowplow_web.snowplow_web_page_views](#model.snowplow_web.snowplow_web_page_views)
- [model.snowplow_web.snowplow_web_sessions_this_run](#model.snowplow_web.snowplow_web_sessions_this_run)
- [model.snowplow_web.snowplow_web_users](#model.snowplow_web.snowplow_web_users)

</TabItem>
<TabItem value="macros" label="Macros">

- [macro.snowplow_utils.materialization_snowplow_incremental_snowflake](#macro.snowplow_utils.materialization_snowplow_incremental_snowflake)

</TabItem>
</Tabs>
</DbtDetails>

### Snowplow Delete From Manifest {#macro.snowplow_utils.snowplow_delete_from_manifest}

<DbtDetails><summary>
<code>macros/utils/snowplow_delete_from_manifest.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro snowplow_delete_from_manifest(models, incremental_manifest_table) %}

  {%- if models is string -%}
    {%- set models = [models] -%}
  {%- endif -%}

  {% if not models|length or not execute %}
    {{ return('') }}
  {% endif %}

  {%- set incremental_manifest_table_exists = adapter.get_relation(incremental_manifest_table.database,
                                                                  incremental_manifest_table.schema,
                                                                  incremental_manifest_table.name) -%}

  {%- if not incremental_manifest_table_exists -%}
    {{return(dbt_utils.log_info("Snowplow: "+incremental_manifest_table|string+" does not exist"))}}
  {%- endif -%}

  {%- set models_in_manifest = dbt_utils.get_column_values(table=incremental_manifest_table, column='model') -%}
  {%- set unmatched_models, matched_models = [], [] -%}

  {%- for model in models -%}

    {%- if model in models_in_manifest -%}
      {%- do matched_models.append(model) -%}
    {%- else -%}
      {%- do unmatched_models.append(model) -%}
    {%- endif -%}

  {%- endfor -%}

  {%- if not matched_models|length -%}
    {{return(dbt_utils.log_info("Snowplow: None of the supplied models exist in the manifest"))}}
  {%- endif -%}

  {% set delete_statement %}
    {%- if target.type in ['databricks', 'spark'] -%}
      delete from {{ incremental_manifest_table }} where model in ({{ snowplow_utils.print_list(matched_models) }});
    {%- else -%}
      -- We don't need transaction but Redshift needs commit statement while BQ does not. By using transaction we cover both.
      begin;
      delete from {{ incremental_manifest_table }} where model in ({{ snowplow_utils.print_list(matched_models) }});
      commit;
    {%- endif -%}
  {% endset %}

  {%- do run_query(delete_statement) -%}

  {%- if matched_models|length -%}
    {% do snowplow_utils.log_message("Snowplow: Deleted models "+snowplow_utils.print_list(matched_models)+" from the manifest") %}
  {%- endif -%}

  {%- if unmatched_models|length -%}
    {% do snowplow_utils.log_message("Snowplow: Models "+snowplow_utils.print_list(unmatched_models)+" do not exist in the manifest") %}
  {%- endif -%}

{% endmacro %}
```

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="macros" label="Macros">

- [macro.snowplow_utils.snowplow_web_delete_from_manifest](#macro.snowplow_utils.snowplow_web_delete_from_manifest)
- [macro.snowplow_utils.snowplow_mobile_delete_from_manifest](#macro.snowplow_utils.snowplow_mobile_delete_from_manifest)

</TabItem>
</Tabs>
</DbtDetails>

### Snowplow Delete Insert {#macro.snowplow_utils.snowplow_delete_insert}

<DbtDetails><summary>
<code>macros/materializations/snowplow_incremental/common/snowplow_delete_insert.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro snowplow_delete_insert(tmp_relation, target_relation, unique_key, upsert_date_key, dest_columns, disable_upsert_lookback) -%}
  {{ adapter.dispatch('snowplow_delete_insert', 'snowplow_utils')(tmp_relation, target_relation, unique_key, upsert_date_key, dest_columns, disable_upsert_lookback) }}
{%- endmacro %}
```

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="macros" label="Macros">

- [macro.snowplow_utils.materialization_snowplow_incremental_default](#macro.snowplow_utils.materialization_snowplow_incremental_default)
- [macro.snowplow_utils.snowplow_snowflake_get_incremental_sql](#macro.snowplow_utils.snowplow_snowflake_get_incremental_sql)

</TabItem>
</Tabs>
</DbtDetails>

### Snowplow Incremental Post Hook {#macro.snowplow_utils.snowplow_incremental_post_hook}

<DbtDetails><summary>
<code>macros/incremental_hooks/snowplow_incremental_post_hook.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro snowplow_incremental_post_hook(package_name) %}
  
  {% set enabled_snowplow_models = snowplow_utils.get_enabled_snowplow_models(package_name) -%}

  {% set successful_snowplow_models = snowplow_utils.get_successful_models(models=enabled_snowplow_models) -%}

  {% set incremental_manifest_table = snowplow_utils.get_incremental_manifest_table_relation(package_name) -%}

  {% set base_events_this_run_table = ref(package_name~'_base_events_this_run') -%}
        
  {{ snowplow_utils.update_incremental_manifest_table(incremental_manifest_table, base_events_this_run_table, successful_snowplow_models) }}                  

{% endmacro %}
```

</DbtDetails>

</DbtDetails>

### Snowplow Is Incremental {#macro.snowplow_utils.snowplow_is_incremental}

<DbtDetails><summary>
<code>macros/materializations/snowplow_incremental/common/snowplow_is_incremental.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro snowplow_is_incremental() %}
  {#-- do not run introspective queries in parsing #}
  {% if not execute %}
    {{ return(False) }}
  {% else %}
    {% set relation = adapter.get_relation(this.database, this.schema, this.table) %}
    {{ return(relation is not none
              and relation.type == 'table'
              and model.config.materialized in ['incremental','snowplow_incremental']
              and not should_full_refresh()) }}
  {% endif %}
{% endmacro %}
```

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="model" label="Models" default>

- [model.snowplow_mobile.snowplow_mobile_base_sessions_lifecycle_manifest](#model.snowplow_mobile.snowplow_mobile_base_sessions_lifecycle_manifest)
- [model.snowplow_web.snowplow_web_base_sessions_lifecycle_manifest](#model.snowplow_web.snowplow_web_base_sessions_lifecycle_manifest)

</TabItem>
<TabItem value="macros" label="Macros">

- [macro.snowplow_utils.is_run_with_new_events](#macro.snowplow_utils.is_run_with_new_events)

</TabItem>
</Tabs>
</DbtDetails>

### Snowplow Merge {#macro.snowplow_utils.snowplow_merge}

<DbtDetails><summary>
<code>macros/materializations/snowplow_incremental/common/snowplow_merge.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro snowplow_merge(tmp_relation, target_relation, unique_key, upsert_date_key, dest_columns, disable_upsert_lookback) -%}
  {{ adapter.dispatch('snowplow_merge', 'snowplow_utils')(tmp_relation, target_relation, unique_key, upsert_date_key, dest_columns, disable_upsert_lookback) }}
{%- endmacro %}
```

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="macros" label="Macros">

- [macro.snowplow_utils.materialization_snowplow_incremental_databricks](#macro.snowplow_utils.materialization_snowplow_incremental_databricks)
- [macro.snowplow_utils.materialization_snowplow_incremental_bigquery](#macro.snowplow_utils.materialization_snowplow_incremental_bigquery)
- [macro.snowplow_utils.snowplow_snowflake_get_incremental_sql](#macro.snowplow_utils.snowplow_snowflake_get_incremental_sql)
- [macro.snowplow_utils.materialization_snowplow_incremental_spark](#macro.snowplow_utils.materialization_snowplow_incremental_spark)

</TabItem>
</Tabs>
</DbtDetails>

### Snowplow Mobile Delete From Manifest {#macro.snowplow_utils.snowplow_mobile_delete_from_manifest}

<DbtDetails><summary>
<code>macros/utils/snowplow_delete_from_manifest.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro snowplow_mobile_delete_from_manifest(models) %}

  {{ snowplow_utils.snowplow_delete_from_manifest(models, ref('snowplow_mobile_incremental_manifest')) }}

{% endmacro %}
```

</DbtDetails>

</DbtDetails>

### Snowplow Snowflake Get Incremental Sql {#macro.snowplow_utils.snowplow_snowflake_get_incremental_sql}

<DbtDetails><summary>
<code>macros/materializations/snowplow_incremental/snowflake/snowplow_incremental.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro snowplow_snowflake_get_incremental_sql(strategy, tmp_relation, target_relation, unique_key, upsert_date_key, dest_columns, disable_upsert_lookback) %}
  {% if strategy == 'merge' %}
    {% do return(snowplow_utils.snowplow_merge(tmp_relation, target_relation, unique_key, upsert_date_key, dest_columns, disable_upsert_lookback)) %}
  {% elif strategy == 'delete+insert' %}
    {% do return(snowplow_utils.snowplow_delete_insert(tmp_relation, target_relation, unique_key, upsert_date_key, dest_columns, disable_upsert_lookback)) %}
  {% else %}
    {% do exceptions.raise_compiler_error('invalid strategy: ' ~ strategy) %}
  {% endif %}
{% endmacro %}
```

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="macros" label="Macros">

- [macro.snowplow_utils.materialization_snowplow_incremental_snowflake](#macro.snowplow_utils.materialization_snowplow_incremental_snowflake)

</TabItem>
</Tabs>
</DbtDetails>

### Snowplow Validate Get Incremental Strategy {#macro.snowplow_utils.snowplow_validate_get_incremental_strategy}

<DbtDetails><summary>
<code>macros/materializations/snowplow_incremental/common/snowplow_validate_get_incremental_strategy.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro snowplow_validate_get_incremental_strategy(config) -%}
  {{ adapter.dispatch('snowplow_validate_get_incremental_strategy', 'snowplow_utils')(config) }}
{%- endmacro %}
```

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="macros" label="Macros">

- [macro.snowplow_utils.materialization_snowplow_incremental_databricks](#macro.snowplow_utils.materialization_snowplow_incremental_databricks)
- [macro.snowplow_utils.materialization_snowplow_incremental_bigquery](#macro.snowplow_utils.materialization_snowplow_incremental_bigquery)
- [macro.snowplow_utils.materialization_snowplow_incremental_snowflake](#macro.snowplow_utils.materialization_snowplow_incremental_snowflake)
- [macro.snowplow_utils.materialization_snowplow_incremental_spark](#macro.snowplow_utils.materialization_snowplow_incremental_spark)

</TabItem>
</Tabs>
</DbtDetails>

### Snowplow Web Delete From Manifest {#macro.snowplow_utils.snowplow_web_delete_from_manifest}

<DbtDetails><summary>
<code>macros/utils/snowplow_delete_from_manifest.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro snowplow_web_delete_from_manifest(models) %}

  {{ snowplow_utils.snowplow_delete_from_manifest(models, ref('snowplow_web_incremental_manifest')) }}

{% endmacro %}
```

</DbtDetails>

</DbtDetails>

### Throw Compiler Error {#macro.snowplow_utils.throw_compiler_error}

<DbtDetails><summary>
<code>macros/utils/throw_compiler_error.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro throw_compiler_error(error_message, disable_error=var("snowplow__disable_errors", false)) %}

  {% if disable_error %}

    {{ return(error_message) }}

  {% else %}

    {{ exceptions.raise_compiler_error(error_message) }}

  {% endif %}

{% endmacro %}
```

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="macros" label="Macros">

- [macro.snowplow_utils.get_level_limit](#macro.snowplow_utils.get_level_limit)

</TabItem>
</Tabs>
</DbtDetails>

### Timestamp Add {#macro.snowplow_utils.timestamp_add}

<DbtDetails><summary>
<code>macros/utils/cross_db/timestamp_functions.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

<Tabs groupId="dispatched_sql">
<TabItem value="raw" label="Raw" default>

```jinja2
{% macro timestamp_add(datepart, interval, tstamp) %}
    {{ return(adapter.dispatch('timestamp_add', 'snowplow_utils')(datepart, interval, tstamp)) }}
{% endmacro %}
```
</TabItem>
<TabItem value="default" label="default">

```jinja2
{% macro default__timestamp_add(datepart, interval, tstamp) %}
    {{ return(dateadd(datepart, interval, tstamp)) }}
{% endmacro %}
```
</TabItem>
<TabItem value="bigquery" label="bigquery">

```jinja2
{% macro bigquery__timestamp_add(datepart, interval, tstamp) %}
    timestamp_add({{tstamp}}, interval {{interval}} {{datepart}})
{% endmacro %}
```
</TabItem>
<TabItem value="databricks" label="databricks">

```jinja2
{% macro databricks__timestamp_add(datepart, interval, tstamp) %}
    timestampadd({{datepart}}, {{interval}}, {{tstamp}})
{% endmacro %}
```
</TabItem>
</Tabs>

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="model" label="Models" default>

- [model.snowplow_web.snowplow_web_base_events_this_run](#model.snowplow_web.snowplow_web_base_events_this_run)
- [model.snowplow_mobile.snowplow_mobile_base_events_this_run](#model.snowplow_mobile.snowplow_mobile_base_events_this_run)
- [model.snowplow_mobile.snowplow_mobile_base_sessions_lifecycle_manifest](#model.snowplow_mobile.snowplow_mobile_base_sessions_lifecycle_manifest)
- [model.snowplow_web.snowplow_web_base_sessions_lifecycle_manifest](#model.snowplow_web.snowplow_web_base_sessions_lifecycle_manifest)
- [model.snowplow_normalize.snowplow_normalize_base_events_this_run](#model.snowplow_normalize.snowplow_normalize_base_events_this_run)

</TabItem>
<TabItem value="macros" label="Macros">

- [macro.snowplow_utils.get_run_limits](#macro.snowplow_utils.get_run_limits)
- [macro.snowplow_utils.return_base_new_event_limits](#macro.snowplow_utils.return_base_new_event_limits)
- [macro.snowplow_utils.get_session_lookback_limit](#macro.snowplow_utils.get_session_lookback_limit)
- [macro.snowplow_utils.get_quarantine_sql](#macro.snowplow_utils.get_quarantine_sql)

</TabItem>
</Tabs>
</DbtDetails>

### Timestamp Diff {#macro.snowplow_utils.timestamp_diff}

<DbtDetails><summary>
<code>macros/utils/cross_db/timestamp_functions.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

<Tabs groupId="dispatched_sql">
<TabItem value="raw" label="Raw" default>

```jinja2
{% macro timestamp_diff(first_tstamp, second_tstamp, datepart) %}
    {{ return(adapter.dispatch('timestamp_diff', 'snowplow_utils')(first_tstamp, second_tstamp, datepart)) }}
{% endmacro %}
```
</TabItem>
<TabItem value="default" label="default">

```jinja2
{% macro default__timestamp_diff(first_tstamp, second_tstamp, datepart) %}
    {{ return(datediff(first_tstamp, second_tstamp, datepart)) }}
{% endmacro %}
```
</TabItem>
<TabItem value="bigquery" label="bigquery">

```jinja2
{% macro bigquery__timestamp_diff(first_tstamp, second_tstamp, datepart) %}
    timestamp_diff({{second_tstamp}}, {{first_tstamp}}, {{datepart}})
{% endmacro %}
```
</TabItem>
</Tabs>

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="model" label="Models" default>

- [model.snowplow_mobile.snowplow_mobile_sessions_aggs](#model.snowplow_mobile.snowplow_mobile_sessions_aggs)
- [model.snowplow_web.snowplow_web_sessions_this_run](#model.snowplow_web.snowplow_web_sessions_this_run)

</TabItem>
</Tabs>
</DbtDetails>

### To Unixtstamp {#macro.snowplow_utils.to_unixtstamp}

<DbtDetails><summary>
<code>macros/utils/cross_db/timestamp_functions.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2



{%- macro to_unixtstamp(tstamp) -%}
    {{ adapter.dispatch('to_unixtstamp', 'snowplow_utils') (tstamp) }}
{%- endmacro %}
```

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="model" label="Models" default>

- [model.snowplow_web.snowplow_web_pv_engaged_time](#model.snowplow_web.snowplow_web_pv_engaged_time)

</TabItem>
</Tabs>
</DbtDetails>

### Tstamp To Str {#macro.snowplow_utils.tstamp_to_str}

<DbtDetails><summary>
<code>macros/utils/tstamp_to_str.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro tstamp_to_str(tstamp) -%}
  '{{ tstamp.strftime("%Y-%m-%d %H:%M:%S") }}'
{%- endmacro %}
```

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="macros" label="Macros">

- [macro.snowplow_utils.print_run_limits](#macro.snowplow_utils.print_run_limits)

</TabItem>
</Tabs>
</DbtDetails>

### Type Max String {#macro.snowplow_utils.type_max_string}

<DbtDetails><summary>
<code>macros/utils/cross_db/datatypes.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

<Tabs groupId="dispatched_sql">
<TabItem value="raw" label="Raw" default>

```jinja2


{%- macro type_max_string() -%}
  {{ return(adapter.dispatch('type_max_string', 'snowplow_utils')()) }}
{%- endmacro -%}


```
</TabItem>
<TabItem value="default" label="default">

```jinja2
{% macro default__type_max_string() %}
    string
{% endmacro %}
```
</TabItem>
<TabItem value="snowflake" label="snowflake">

```jinja2
{% macro snowflake__type_max_string() %}
    varchar
{% endmacro %}
```
</TabItem>
<TabItem value="redshift" label="redshift">

```jinja2
{% macro redshift__type_max_string() %}
    varchar(max)
{% endmacro %}
```
</TabItem>
<TabItem value="postgres" label="postgres">

```jinja2
{% macro postgres__type_max_string() %}
    text
{% endmacro %}
```
</TabItem>
</Tabs>

</DbtDetails>

</DbtDetails>

### Type String {#macro.snowplow_utils.type_string}

<DbtDetails><summary>
<code>macros/utils/cross_db/datatypes.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

<Tabs groupId="dispatched_sql">
<TabItem value="raw" label="Raw" default>

```jinja2


{%- macro type_string(max_characters) -%}
  {{ return(adapter.dispatch('type_string', 'snowplow_utils')(max_characters)) }}
{%- endmacro -%}


```
</TabItem>
<TabItem value="default" label="default">

```jinja2
{% macro default__type_string(max_characters) %}
    varchar( {{max_characters }} )
{% endmacro %}
```
</TabItem>
<TabItem value="bigquery" label="bigquery">

```jinja2
{% macro bigquery__type_string(max_characters) %}
    string
{% endmacro %}
```
</TabItem>
</Tabs>

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="model" label="Models" default>

- [model.snowplow_mobile.snowplow_mobile_incremental_manifest](#model.snowplow_mobile.snowplow_mobile_incremental_manifest)
- [model.snowplow_mobile.snowplow_mobile_sessions_this_run](#model.snowplow_mobile.snowplow_mobile_sessions_this_run)
- [model.snowplow_web.snowplow_web_incremental_manifest](#model.snowplow_web.snowplow_web_incremental_manifest)
- [model.snowplow_web.snowplow_web_base_quarantined_sessions](#model.snowplow_web.snowplow_web_base_quarantined_sessions)
- [model.snowplow_normalize.snowplow_normalize_incremental_manifest](#model.snowplow_normalize.snowplow_normalize_incremental_manifest)
- [model.snowplow_web.snowplow_web_sessions_this_run](#model.snowplow_web.snowplow_web_sessions_this_run)

</TabItem>
</Tabs>
</DbtDetails>

### Unnest {#macro.snowplow_utils.unnest}

<DbtDetails><summary>
<code>macros/utils/cross_db/unnest.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

<Tabs groupId="dispatched_sql">
<TabItem value="raw" label="Raw" default>

```jinja2


{%- macro unnest(id_column, unnest_column, field_alias, source_table) -%}
    {{ return(adapter.dispatch('unnest', 'snowplow_utils')(id_column, unnest_column, field_alias, source_table)) }}
{%- endmacro -%}


```
</TabItem>
<TabItem value="default" label="default">

```jinja2
{% macro default__unnest(id_column, unnest_column, field_alias, source_table) %}
    select {{ id_column }}, explode({{ unnest_column }}) as {{ field_alias }}
    from {{ source_table }}
{% endmacro %}
```
</TabItem>
<TabItem value="snowflake" label="snowflake">

```jinja2
{% macro snowflake__unnest(id_column, unnest_column, field_alias, source_table) %}
    select t.{{ id_column }}, r.value as {{ field_alias }}
    from {{ source_table }} t, table(flatten(t.{{ unnest_column }})) r
{% endmacro %}
```
</TabItem>
<TabItem value="bigquery" label="bigquery">

```jinja2
{% macro bigquery__unnest(id_column, unnest_column, field_alias, source_table) %}
    select {{ id_column }}, r as {{ field_alias }}
    from {{ source_table }} t, unnest(t.{{ unnest_column }}) r
{% endmacro %}
```
</TabItem>
<TabItem value="redshift" label="redshift">

```jinja2
{% macro redshift__unnest(id_column, unnest_column, field_alias, source_table) %}
    select {{ id_column }}, {{ field_alias }}
    from {{ source_table }} p, p.{{ unnest_column }} as {{ field_alias }}
{% endmacro %}
```
</TabItem>
<TabItem value="postgres" label="postgres">

```jinja2
{% macro postgres__unnest(id_column, unnest_column, field_alias, source_table) %}
    select {{ id_column }}, cast(trim(unnest({{ unnest_column }})) as {{ type_int() }}) as {{ field_alias }}
    from {{ source_table }}
{% endmacro %}
```
</TabItem>
</Tabs>

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="model" label="Models" default>

- [model.snowplow_media_player.snowplow_media_player_media_stats](#model.snowplow_media_player.snowplow_media_player_media_stats)

</TabItem>
</Tabs>
</DbtDetails>

### Update Incremental Manifest Table {#macro.snowplow_utils.update_incremental_manifest_table}

<DbtDetails><summary>
<code>macros/incremental_hooks/update_incremental_manifest_table.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

<Tabs groupId="dispatched_sql">
<TabItem value="raw" label="Raw" default>

```jinja2
{% macro update_incremental_manifest_table(manifest_table, base_events_table, models) -%}

  {{ return(adapter.dispatch('update_incremental_manifest_table', 'snowplow_utils')(manifest_table, base_events_table, models)) }}

{% endmacro %}
```
</TabItem>
<TabItem value="default" label="default">

```jinja2
{% macro default__update_incremental_manifest_table(manifest_table, base_events_table, models) -%}

  {% if models %}

    {% set last_success_query %}
      select 
        b.model, 
        a.last_success 

      from 
        (select max(collector_tstamp) as last_success from {{ base_events_table }}) a,
        ({% for model in models %} select '{{model}}' as model {%- if not loop.last %} union all {% endif %} {% endfor %}) b

      where a.last_success is not null -- if run contains no data don't add to manifest
    {% endset %}

    merge into {{ manifest_table }} m
    using ( {{ last_success_query }} ) s
    on m.model = s.model
    when matched then
        update set last_success = greatest(m.last_success, s.last_success)
    when not matched then
        insert (model, last_success) values(model, last_success);

    {% if target.type == 'snowflake' %}
      commit;
    {% endif %}
    
  {% endif %}

{%- endmacro %}
```
</TabItem>
<TabItem value="postgres" label="postgres">

```jinja2
{% macro postgres__update_incremental_manifest_table(manifest_table, base_events_table, models) -%}

  {% if models %}

    begin transaction;
      --temp table to find the greatest last_success per model.
      --this protects against partial backfills causing the last_success to move back in time.
      create temporary table snowplow_models_last_success as (
        select
          a.model,
          greatest(a.last_success, b.last_success) as last_success

        from (

          select
            model,
            last_success

          from
            (select max(collector_tstamp) as last_success from {{ base_events_table }}) as ls,
            ({% for model in models %} select '{{model}}' as model {%- if not loop.last %} union all {% endif %} {% endfor %}) as mod

          where last_success is not null -- if run contains no data don't add to manifest

        ) a
        left join {{ manifest_table }} b
        on a.model = b.model
        );

      delete from {{ manifest_table }} where model in (select model from snowplow_models_last_success);
      insert into {{ manifest_table }} (select * from snowplow_models_last_success);

    end transaction;

    drop table snowplow_models_last_success;
    
  {% endif %}

{%- endmacro %}
```
</TabItem>
</Tabs>

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="macros" label="Macros">

- [macro.snowplow_utils.snowplow_incremental_post_hook](#macro.snowplow_utils.snowplow_incremental_post_hook)

</TabItem>
</Tabs>
</DbtDetails>


## Snowplow Web
### Snowplow Web Delete From Manifest {#macro.snowplow_utils.snowplow_web_delete_from_manifest}

<DbtDetails><summary>
<code>macros/utils/snowplow_delete_from_manifest.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro snowplow_web_delete_from_manifest(models) %}

  {{ snowplow_utils.snowplow_delete_from_manifest(models, ref('snowplow_web_incremental_manifest')) }}

{% endmacro %}
```

</DbtDetails>

</DbtDetails>

### Allow Refresh {#macro.snowplow_web.allow_refresh}

<DbtDetails><summary>
<code>macros/allow_refresh.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

<Tabs groupId="dispatched_sql">
<TabItem value="raw" label="Raw" default>

```jinja2
{% macro allow_refresh() %}
  {{ return(adapter.dispatch('allow_refresh', 'snowplow_web')()) }}
{% endmacro %}
```
</TabItem>
<TabItem value="default" label="default">

```jinja2
{% macro default__allow_refresh() %}
  
  {% set allow_refresh = snowplow_utils.get_value_by_target(
                                    dev_value=none,
                                    default_value=var('snowplow__allow_refresh'),
                                    dev_target_name=var('snowplow__dev_target_name')
                                    ) %}

  {{ return(allow_refresh) }}

{% endmacro %}
```
</TabItem>
</Tabs>

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="model" label="Models" default>

- [model.snowplow_web.snowplow_web_incremental_manifest](#model.snowplow_web.snowplow_web_incremental_manifest)
- [model.snowplow_web.snowplow_web_base_quarantined_sessions](#model.snowplow_web.snowplow_web_base_quarantined_sessions)
- [model.snowplow_web.snowplow_web_base_sessions_lifecycle_manifest](#model.snowplow_web.snowplow_web_base_sessions_lifecycle_manifest)

</TabItem>
</Tabs>
</DbtDetails>

### Filter Bots {#macro.snowplow_web.filter_bots}

<DbtDetails><summary>
<code>macros/filter_bots.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

<Tabs groupId="dispatched_sql">
<TabItem value="raw" label="Raw" default>

```jinja2
{% macro filter_bots() %}
  {{ return(adapter.dispatch('filter_bots', 'snowplow_web')()) }}
{%- endmacro -%}


```
</TabItem>
<TabItem value="default" label="default">

```jinja2
{% macro default__filter_bots() %}
  and ev.useragent not similar to '%(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|PingdomBot|PhantomJS|YandexBot|Twitterbot|a_archiver|facebookexternalhit|Bingbot|BingPreview|Googlebot|Baiduspider|360(Spider|User-agent)|semalt)%'
{% endmacro %}
```
</TabItem>
<TabItem value="snowflake" label="snowflake">

```jinja2
{% macro snowflake__filter_bots() %}
  and not rlike(ev.useragent, '.*(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|PingdomBot|PhantomJS|YandexBot|Twitterbot|a_archiver|facebookexternalhit|Bingbot|BingPreview|Googlebot|Baiduspider|360(Spider|User-agent)|semalt).*')
{% endmacro %}
```
</TabItem>
<TabItem value="bigquery" label="bigquery">

```jinja2
{% macro bigquery__filter_bots() %}
  and not regexp_contains(ev.useragent, '(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|PingdomBot|PhantomJS|YandexBot|Twitterbot|a_archiver|facebookexternalhit|Bingbot|BingPreview|Googlebot|Baiduspider|360(Spider|User-agent)|semalt)')
{% endmacro %}
```
</TabItem>
<TabItem value="databricks" label="databricks">

```jinja2
{% macro databricks__filter_bots() %}
  and not rlike(ev.useragent, '.*(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|PingdomBot|PhantomJS|YandexBot|Twitterbot|a_archiver|facebookexternalhit|Bingbot|BingPreview|Googlebot|Baiduspider|360(Spider|User-agent)|semalt).*')
{% endmacro %}
```
</TabItem>
<TabItem value="redshift" label="redshift">

```jinja2
{% macro redshift__filter_bots() %}
  and ev.useragent not similar to '%(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|PingdomBot|PhantomJS|YandexBot|Twitterbot|a_archiver|facebookexternalhit|Bingbot|BingPreview|Googlebot|Baiduspider|360(Spider|User-agent)|semalt)%'
{% endmacro %}
```
</TabItem>
</Tabs>

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="model" label="Models" default>

- [model.snowplow_web.snowplow_web_page_views_this_run](#model.snowplow_web.snowplow_web_page_views_this_run)
- [model.snowplow_web.snowplow_web_page_view_events](#model.snowplow_web.snowplow_web_page_view_events)

</TabItem>
</Tabs>
</DbtDetails>

### Iab Fields {#macro.snowplow_web.iab_fields}

<DbtDetails><summary>
<code>macros/bigquery/page_view_contexts.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro iab_fields() %}
  
  {% set iab_fields = [
      {'field':'category', 'dtype':'string'},
      {'field':'primary_impact', 'dtype':'string'},
      {'field':'reason', 'dtype':'string'},
      {'field':'spider_or_robot', 'dtype':'boolean'}
    ] %}

  {{ return(iab_fields) }}

{% endmacro %}
```

</DbtDetails>

</DbtDetails>

### Stitch User Identifiers {#macro.snowplow_web.stitch_user_identifiers}

<DbtDetails><summary>
<code>macros/stitch_user_identifiers.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro stitch_user_identifiers(enabled, relation=this, user_mapping_relation=ref('snowplow_web_user_mapping')) %}

  {% if enabled and target.type not in ['databricks', 'spark'] | as_bool() %}

    -- Update sessions table with mapping
    update {{ relation }} as s
    set stitched_user_id = um.user_id
    from {{ user_mapping_relation }} as um
    where s.domain_userid = um.domain_userid;

  {% elif enabled and target.type in ['databricks', 'spark']  | as_bool() %}

    -- Update sessions table with mapping
    merge into {{ relation }} as s
    using {{ user_mapping_relation }} as um
    on s.domain_userid = um.domain_userid
    when matched then 
      update set s.stitched_user_id = um.user_id;

  {% endif %}

{% endmacro %}
```

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="model" label="Models" default>

- [model.snowplow_web.snowplow_web_sessions](#model.snowplow_web.snowplow_web_sessions)

</TabItem>
</Tabs>
</DbtDetails>

### Ua Fields {#macro.snowplow_web.ua_fields}

<DbtDetails><summary>
<code>macros/bigquery/page_view_contexts.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro ua_fields() %}
  
  {% set ua_fields = [
      {'field': 'useragent_family', 'dtype': 'string'},
      {'field': 'useragent_major', 'dtype': 'string'},
      {'field': 'useragent_minor', 'dtype': 'string'},
      {'field': 'useragent_patch', 'dtype': 'string'},
      {'field': 'useragent_version', 'dtype': 'string'},
      {'field': 'os_family', 'dtype': 'string'},
      {'field': 'os_major', 'dtype': 'string'},
      {'field': 'os_minor', 'dtype': 'string'},
      {'field': 'os_patch', 'dtype': 'string'},
      {'field': 'os_patch_minor', 'dtype': 'string'},
      {'field': 'os_version', 'dtype': 'string'},
      {'field': 'device_family', 'dtype': 'string'}
    ] %}

  {{ return(ua_fields) }}

{% endmacro %}
```

</DbtDetails>

</DbtDetails>

### Web Cluster By Fields Consent {#macro.snowplow_web.web_cluster_by_fields_consent}

<DbtDetails><summary>
<code>macros/cluster_by_fields.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

<Tabs groupId="dispatched_sql">
<TabItem value="raw" label="Raw" default>

```jinja2
{% macro web_cluster_by_fields_consent() %}

  {{ return(adapter.dispatch('web_cluster_by_fields_consent', 'snowplow_web')()) }}

{% endmacro %}
```
</TabItem>
<TabItem value="default" label="default">

```jinja2
{% macro default__web_cluster_by_fields_consent() %}

  {{ return(snowplow_utils.get_cluster_by(bigquery_cols=["event_id","domain_userid"], snowflake_cols=["to_date(load_tstamp)"])) }}

{% endmacro %}
```
</TabItem>
</Tabs>

</DbtDetails>

</DbtDetails>

### Web Cluster By Fields Page Views {#macro.snowplow_web.web_cluster_by_fields_page_views}

<DbtDetails><summary>
<code>macros/cluster_by_fields.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

<Tabs groupId="dispatched_sql">
<TabItem value="raw" label="Raw" default>

```jinja2
{% macro web_cluster_by_fields_page_views() %}

  {{ return(adapter.dispatch('web_cluster_by_fields_page_views', 'snowplow_web')()) }}

{% endmacro %}
```
</TabItem>
<TabItem value="default" label="default">

```jinja2
{% macro default__web_cluster_by_fields_page_views() %}

  {{ return(snowplow_utils.get_cluster_by(bigquery_cols=["domain_userid","domain_sessionid"], snowflake_cols=["to_date(start_tstamp)"])) }}

{% endmacro %}
```
</TabItem>
</Tabs>

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="model" label="Models" default>

- [model.snowplow_web.snowplow_web_page_views](#model.snowplow_web.snowplow_web_page_views)

</TabItem>
</Tabs>
</DbtDetails>

### Web Cluster By Fields Sessions {#macro.snowplow_web.web_cluster_by_fields_sessions}

<DbtDetails><summary>
<code>macros/cluster_by_fields.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

<Tabs groupId="dispatched_sql">
<TabItem value="raw" label="Raw" default>

```jinja2
{% macro web_cluster_by_fields_sessions() %}

  {{ return(adapter.dispatch('web_cluster_by_fields_sessions', 'snowplow_web')()) }}

{% endmacro %}
```
</TabItem>
<TabItem value="default" label="default">

```jinja2
{% macro default__web_cluster_by_fields_sessions() %}

  {{ return(snowplow_utils.get_cluster_by(bigquery_cols=["domain_userid"], snowflake_cols=["to_date(start_tstamp)"])) }}

{% endmacro %}
```
</TabItem>
</Tabs>

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="model" label="Models" default>

- [model.snowplow_web.snowplow_web_sessions](#model.snowplow_web.snowplow_web_sessions)

</TabItem>
</Tabs>
</DbtDetails>

### Web Cluster By Fields Sessions Lifecycle {#macro.snowplow_web.web_cluster_by_fields_sessions_lifecycle}

<DbtDetails><summary>
<code>macros/cluster_by_fields.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

<Tabs groupId="dispatched_sql">
<TabItem value="raw" label="Raw" default>

```jinja2
{% macro web_cluster_by_fields_sessions_lifecycle() %}

  {{ return(adapter.dispatch('web_cluster_by_fields_sessions_lifecycle', 'snowplow_web')()) }}

{% endmacro %}
```
</TabItem>
<TabItem value="default" label="default">

```jinja2
{% macro default__web_cluster_by_fields_sessions_lifecycle() %}

  {{ return(snowplow_utils.get_cluster_by(bigquery_cols=["session_id"], snowflake_cols=["to_date(start_tstamp)"])) }}

{% endmacro %}
```
</TabItem>
</Tabs>

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="model" label="Models" default>

- [model.snowplow_web.snowplow_web_base_sessions_lifecycle_manifest](#model.snowplow_web.snowplow_web_base_sessions_lifecycle_manifest)

</TabItem>
</Tabs>
</DbtDetails>

### Web Cluster By Fields Users {#macro.snowplow_web.web_cluster_by_fields_users}

<DbtDetails><summary>
<code>macros/cluster_by_fields.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

<Tabs groupId="dispatched_sql">
<TabItem value="raw" label="Raw" default>

```jinja2
{% macro web_cluster_by_fields_users() %}

  {{ return(adapter.dispatch('web_cluster_by_fields_users', 'snowplow_web')()) }}

{% endmacro %}
```
</TabItem>
<TabItem value="default" label="default">

```jinja2
{% macro default__web_cluster_by_fields_users() %}

  {{ return(snowplow_utils.get_cluster_by(bigquery_cols=["user_id","domain_userid"], snowflake_cols=["to_date(start_tstamp)"])) }}

{% endmacro %}
```
</TabItem>
</Tabs>

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="model" label="Models" default>

- [model.snowplow_web.snowplow_web_users](#model.snowplow_web.snowplow_web_users)

</TabItem>
</Tabs>
</DbtDetails>

### Yauaa Fields {#macro.snowplow_web.yauaa_fields}

<DbtDetails><summary>
<code>macros/bigquery/page_view_contexts.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro yauaa_fields() %}
  
  {% set yauaa_fields = [
      {'field': 'device_class', 'dtype': 'string'},
      {'field': 'agent_class', 'dtype': 'string'},
      {'field': 'agent_name', 'dtype': 'string'},
      {'field': 'agent_name_version', 'dtype': 'string'},
      {'field': 'agent_name_version_major', 'dtype': 'string'},
      {'field': 'agent_version', 'dtype': 'string'},
      {'field': 'agent_version_major', 'dtype': 'string'},
      {'field': 'device_brand', 'dtype': 'string'},
      {'field': 'device_name', 'dtype': 'string'},
      {'field': 'device_version', 'dtype': 'string'},
      {'field': 'layout_engine_class', 'dtype': 'string'},
      {'field': 'layout_engine_name', 'dtype': 'string'},
      {'field': 'layout_engine_name_version', 'dtype': 'string'},
      {'field': 'layout_engine_name_version_major', 'dtype': 'string'},
      {'field': 'layout_engine_version', 'dtype': 'string'},
      {'field': 'layout_engine_version_major', 'dtype': 'string'},
      {'field': 'operating_system_class', 'dtype': 'string'},
      {'field': 'operating_system_name', 'dtype': 'string'},
      {'field': 'operating_system_name_version', 'dtype': 'string'},
      {'field': 'operating_system_version', 'dtype': 'string'}
    ] %}

  {{ return(yauaa_fields) }}

{% endmacro %}
```

</DbtDetails>

</DbtDetails>

### Edge Cases To Ignore {#macro.snowplow_web_integration_tests.edge_cases_to_ignore}

<DbtDetails><summary>
<code>macros/edge_cases_to_ignore.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro edge_cases_to_ignore() %}
  user_id not in (
    'stray page ping', -- Known unsolved issue https://github.com/snowplow/data-models/issues/92
    'NULL domain_userid' -- Case when `domain_userid` is null but `domain_sessionid` is not null. Shouldn't happen. Will solve if it arises.
    )
{% endmacro %}
```

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="model" label="Models" default>

- [model.snowplow_web_integration_tests.snowplow_web_events_stg](#model.snowplow_web_integration_tests.snowplow_web_events_stg)

</TabItem>
</Tabs>
</DbtDetails>


## Snowplow Mobile
### Allow Refresh {#macro.snowplow_mobile.allow_refresh}

<DbtDetails><summary>
<code>macros/allow_refresh.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

<Tabs groupId="dispatched_sql">
<TabItem value="raw" label="Raw" default>

```jinja2
{% macro allow_refresh() %}
  {{ return(adapter.dispatch('allow_refresh', 'snowplow_mobile')()) }}
{% endmacro %}
```
</TabItem>
<TabItem value="default" label="default">

```jinja2
{% macro default__allow_refresh() %}
  
  {% set allow_refresh = snowplow_utils.get_value_by_target(
                                    dev_value=none,
                                    default_value=var('snowplow__allow_refresh'),
                                    dev_target_name=var('snowplow__dev_target_name')
                                    ) %}

  {{ return(allow_refresh) }}

{% endmacro %}
```
</TabItem>
</Tabs>

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="model" label="Models" default>

- [model.snowplow_mobile.snowplow_mobile_incremental_manifest](#model.snowplow_mobile.snowplow_mobile_incremental_manifest)
- [model.snowplow_mobile.snowplow_mobile_base_sessions_lifecycle_manifest](#model.snowplow_mobile.snowplow_mobile_base_sessions_lifecycle_manifest)

</TabItem>
</Tabs>
</DbtDetails>

### App Context Fields {#macro.snowplow_mobile.app_context_fields}

<DbtDetails><summary>
<code>macros/bigquery/context_fields.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro app_context_fields() %}

  {% set app_context_fields = [
    {'field':'build', 'dtype':'string'},
    {'field':'version', 'dtype':'string'}
    ] %}

  {{ return(app_context_fields) }}

{% endmacro %}
```

</DbtDetails>

</DbtDetails>

### App Error Context Fields {#macro.snowplow_mobile.app_error_context_fields}

<DbtDetails><summary>
<code>macros/bigquery/context_fields.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro app_error_context_fields() %}

  {% set app_error_context_fields = [
    {'field':'message', 'dtype':'string'},
    {'field':'programming_language', 'dtype':'string'},
    {'field':'class_name', 'dtype':'string'},
    {'field':'exception_name', 'dtype':'string'},
    {'field':'file_name', 'dtype':'string'},
    {'field':'is_fatal', 'dtype':'boolean'},
    {'field':'line_column', 'dtype':'integer'},
    {'field':'line_number', 'dtype':'integer'},
    {'field':'stack_trace', 'dtype':'string'},
    {'field':'thread_id', 'dtype':'integer'},
    {'field':'thread_name', 'dtype':'string'}
    ] %}

  {{ return(app_error_context_fields) }}

{% endmacro %}
```

</DbtDetails>

</DbtDetails>

### Bool Or {#macro.snowplow_mobile.bool_or}

<DbtDetails><summary>
<code>macros/bool_or.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

<Tabs groupId="dispatched_sql">
<TabItem value="raw" label="Raw" default>

```jinja2
{% macro bool_or(field) %}

  {{ return(adapter.dispatch('bool_or', 'snowplow_mobile')(field)) }}

{% endmacro %}
```
</TabItem>
<TabItem value="default" label="default">

```jinja2
{% macro default__bool_or(field) %}

  BOOL_OR(
    {{ field }}
  )

{% endmacro %}
```
</TabItem>
<TabItem value="snowflake" label="snowflake">

```jinja2
{% macro snowflake__bool_or(field) %}

  BOOLOR_AGG(
    {{ field }}
  )

{% endmacro %}
```
</TabItem>
<TabItem value="bigquery" label="bigquery">

```jinja2
{% macro bigquery__bool_or(field) %}

  LOGICAL_OR(
    {{ field }}
  )

{% endmacro %}
```
</TabItem>
</Tabs>

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="model" label="Models" default>

- [model.snowplow_mobile.snowplow_mobile_sessions_aggs](#model.snowplow_mobile.snowplow_mobile_sessions_aggs)

</TabItem>
</Tabs>
</DbtDetails>

### Cluster By Fields App Errors {#macro.snowplow_mobile.cluster_by_fields_app_errors}

<DbtDetails><summary>
<code>macros/cluster_by_fields.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

<Tabs groupId="dispatched_sql">
<TabItem value="raw" label="Raw" default>

```jinja2
{% macro cluster_by_fields_app_errors() %}

  {{ return(adapter.dispatch('cluster_by_fields_app_errors', 'snowplow_mobile')()) }}

{% endmacro %}
```
</TabItem>
<TabItem value="default" label="default">

```jinja2
{% macro default__cluster_by_fields_app_errors() %}

  {{ return(snowplow_utils.get_cluster_by(bigquery_cols=["session_id"], snowflake_cols=["to_date(derived_tstamp)"])) }}

{% endmacro %}
```
</TabItem>
</Tabs>

</DbtDetails>

</DbtDetails>

### Geo Context Fields {#macro.snowplow_mobile.geo_context_fields}

<DbtDetails><summary>
<code>macros/bigquery/context_fields.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro geo_context_fields() %}

  {% set geo_context_fields = [
    {'field':('latitude', 'device_latitude'), 'dtype':'float64'},
    {'field':('longitude', 'device_longitude'), 'dtype':'float64'},
    {'field':('latitude_longitude_accuracy', 'device_latitude_longitude_accuracy'), 'dtype':'float64'},
    {'field':('altitude', 'device_altitude'), 'dtype':'float64'},
    {'field':('altitude_accuracy', 'device_altitude_accuracy'), 'dtype':'float64'},
    {'field':('bearing', 'device_bearing'), 'dtype':'float64'},
    {'field':('speed', 'device_speed'), 'dtype':'float64'}
    ] %}

  {{ return(geo_context_fields) }}

{% endmacro %}
```

</DbtDetails>

</DbtDetails>

### Get Device User Id Path Sql {#macro.snowplow_mobile.get_device_user_id_path_sql}

<DbtDetails><summary>
<code>macros/get_path_sql.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

<Tabs groupId="dispatched_sql">
<TabItem value="raw" label="Raw" default>

```jinja2
{% macro get_device_user_id_path_sql(relation_alias) %}

  {{ return(adapter.dispatch('get_device_user_id_path_sql', 'snowplow_mobile')(relation_alias)) }}

{% endmacro %}
```
</TabItem>
<TabItem value="default" label="default">

```jinja2
{% macro default__get_device_user_id_path_sql(relation_alias) %}

  {% set user_id %}
    {{ relation_alias }}.contexts_com_snowplowanalytics_snowplow_client_session_1[0]:userId::VARCHAR(36)
  {% endset %}

  {{ return(user_id) }}

{% endmacro %}
```
</TabItem>
<TabItem value="bigquery" label="bigquery">

```jinja2
{% macro bigquery__get_device_user_id_path_sql(relation_alias) %}

-- setting relation through variable is not currently supported (recognised as string), different logic for integration tests
{% if target.schema.startswith('gh_sp_mobile_dbt_') %}

  {%- set relation=ref('snowplow_mobile_events_stg') %}

{% else %}

  {%- set relation=source('atomic','events') %}

{% endif %}

  {%- set user_id = snowplow_utils.combine_column_versions(
                                  relation=relation,
                                  column_prefix='contexts_com_snowplowanalytics_snowplow_client_session_1_',
                                  required_fields=['user_id'],
                                  relation_alias=relation_alias,
                                  include_field_alias=false
                                  )|join('') -%}

  {{ return(user_id) }}

{% endmacro %}
```
</TabItem>
<TabItem value="databricks" label="databricks">

```jinja2
{% macro databricks__get_device_user_id_path_sql(relation_alias) %}

  {% set user_id %}
    {{ relation_alias }}.contexts_com_snowplowanalytics_snowplow_client_session_1[0].user_id::STRING
  {% endset %}

  {{ return(user_id) }}

{% endmacro %}
```
</TabItem>
<TabItem value="spark" label="spark">

```jinja2
{% macro spark__get_device_user_id_path_sql(relation_alias) %}

  {{ return(snowplow_mobile.databricks__get_device_user_id_path_sql(relation_alias)) }}

{% endmacro %}
```
</TabItem>
</Tabs>

</DbtDetails>

</DbtDetails>

### Get Session Id Path Sql {#macro.snowplow_mobile.get_session_id_path_sql}

<DbtDetails><summary>
<code>macros/get_path_sql.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

<Tabs groupId="dispatched_sql">
<TabItem value="raw" label="Raw" default>

```jinja2
{% macro get_session_id_path_sql(relation_alias) %}

  {{ return(adapter.dispatch('get_session_id_path_sql', 'snowplow_mobile')(relation_alias)) }}

{% endmacro %}
```
</TabItem>
<TabItem value="default" label="default">

```jinja2
{% macro default__get_session_id_path_sql(relation_alias) %}
  {% set session_id %}
    {{ relation_alias }}.contexts_com_snowplowanalytics_snowplow_client_session_1[0]:sessionId::VARCHAR(36)
  {% endset %}

  {{ return(session_id) }}

{% endmacro %}
```
</TabItem>
<TabItem value="bigquery" label="bigquery">

```jinja2
{% macro bigquery__get_session_id_path_sql(relation_alias) %}

-- setting relation through variable is not currently supported (recognised as string), different logic for integration tests
{% if target.schema.startswith('gh_sp_mobile_dbt_') %}

  {%- set relation=ref('snowplow_mobile_events_stg') %}

{% else %}

  {%- set relation=source('atomic','events') %}

{% endif %}

  {%- set session_id = snowplow_utils.combine_column_versions(
                                  relation=relation,
                                  column_prefix='contexts_com_snowplowanalytics_snowplow_client_session_1_',
                                  required_fields=['session_id'],
                                  relation_alias=relation_alias,
                                  include_field_alias=false
                                  )|join('') -%}


  {{ return(session_id) }}

{% endmacro %}
```
</TabItem>
<TabItem value="databricks" label="databricks">

```jinja2
{% macro databricks__get_session_id_path_sql(relation_alias) %}
  {% set session_id %}
    {{ relation_alias }}.contexts_com_snowplowanalytics_snowplow_client_session_1[0].session_id::STRING
  {% endset %}

  {{ return(session_id) }}

{% endmacro %}
```
</TabItem>
<TabItem value="spark" label="spark">

```jinja2
{% macro spark__get_session_id_path_sql(relation_alias) %}

  {{ return(snowplow_mobile.databricks__get_session_id_path_sql(relation_alias)) }}

{% endmacro %}
```
</TabItem>
</Tabs>

</DbtDetails>

</DbtDetails>

### Mobile Cluster By Fields Screen Views {#macro.snowplow_mobile.mobile_cluster_by_fields_screen_views}

<DbtDetails><summary>
<code>macros/cluster_by_fields.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

<Tabs groupId="dispatched_sql">
<TabItem value="raw" label="Raw" default>

```jinja2
{% macro mobile_cluster_by_fields_screen_views() %}

  {{ return(adapter.dispatch('mobile_cluster_by_fields_screen_views', 'snowplow_mobile')()) }}

{% endmacro %}
```
</TabItem>
<TabItem value="default" label="default">

```jinja2
{% macro default__mobile_cluster_by_fields_screen_views() %}

  {{ return(snowplow_utils.get_cluster_by(bigquery_cols=["device_user_id", "session_id"], snowflake_cols=["to_date(derived_tstamp)"])) }}

{% endmacro %}
```
</TabItem>
</Tabs>

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="model" label="Models" default>

- [model.snowplow_mobile.snowplow_mobile_screen_views](#model.snowplow_mobile.snowplow_mobile_screen_views)

</TabItem>
</Tabs>
</DbtDetails>

### Mobile Cluster By Fields Sessions {#macro.snowplow_mobile.mobile_cluster_by_fields_sessions}

<DbtDetails><summary>
<code>macros/cluster_by_fields.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

<Tabs groupId="dispatched_sql">
<TabItem value="raw" label="Raw" default>

```jinja2
{% macro mobile_cluster_by_fields_sessions() %}

  {{ return(adapter.dispatch('mobile_cluster_by_fields_sessions', 'snowplow_mobile')()) }}

{% endmacro %}
```
</TabItem>
<TabItem value="default" label="default">

```jinja2
{% macro default__mobile_cluster_by_fields_sessions() %}

  {{ return(snowplow_utils.get_cluster_by(bigquery_cols=["device_user_id"], snowflake_cols=["to_date(start_tstamp)"])) }}

{% endmacro %}
```
</TabItem>
</Tabs>

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="model" label="Models" default>

- [model.snowplow_mobile.snowplow_mobile_sessions](#model.snowplow_mobile.snowplow_mobile_sessions)

</TabItem>
</Tabs>
</DbtDetails>

### Mobile Cluster By Fields Sessions Lifecycle {#macro.snowplow_mobile.mobile_cluster_by_fields_sessions_lifecycle}

<DbtDetails><summary>
<code>macros/cluster_by_fields.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

<Tabs groupId="dispatched_sql">
<TabItem value="raw" label="Raw" default>

```jinja2
{% macro mobile_cluster_by_fields_sessions_lifecycle() %}

  {{ return(adapter.dispatch('mobile_cluster_by_fields_sessions_lifecycle', 'snowplow_mobile')()) }}

{% endmacro %}
```
</TabItem>
<TabItem value="default" label="default">

```jinja2
{% macro default__mobile_cluster_by_fields_sessions_lifecycle() %}

  {{ return(snowplow_utils.get_cluster_by(bigquery_cols=["session_id"], snowflake_cols=["to_date(start_tstamp)"])) }}

{% endmacro %}
```
</TabItem>
</Tabs>

</DbtDetails>

</DbtDetails>

### Mobile Cluster By Fields Users {#macro.snowplow_mobile.mobile_cluster_by_fields_users}

<DbtDetails><summary>
<code>macros/cluster_by_fields.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

<Tabs groupId="dispatched_sql">
<TabItem value="raw" label="Raw" default>

```jinja2
{% macro mobile_cluster_by_fields_users() %}

  {{ return(adapter.dispatch('mobile_cluster_by_fields_users', 'snowplow_mobile')()) }}

{% endmacro %}
```
</TabItem>
<TabItem value="default" label="default">

```jinja2
{% macro default__mobile_cluster_by_fields_users() %}

  {{ return(snowplow_utils.get_cluster_by(bigquery_cols=["device_user_id"], snowflake_cols=["to_date(start_tstamp)"])) }}

{% endmacro %}
```
</TabItem>
</Tabs>

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="model" label="Models" default>

- [model.snowplow_mobile.snowplow_mobile_users](#model.snowplow_mobile.snowplow_mobile_users)

</TabItem>
</Tabs>
</DbtDetails>

### Mobile Context Fields {#macro.snowplow_mobile.mobile_context_fields}

<DbtDetails><summary>
<code>macros/bigquery/context_fields.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro mobile_context_fields() %}

  {% set mobile_context_fields = [
    {'field':'device_manufacturer', 'dtype':'string'},
    {'field':'device_model', 'dtype':'string'},
    {'field':'os_type', 'dtype':'string'},
    {'field':'os_version', 'dtype':'string'},
    {'field':'android_idfa', 'dtype':'string'},
    {'field':'apple_idfa', 'dtype':'string'},
    {'field':'apple_idfv', 'dtype':'string'},
    {'field':'carrier', 'dtype':'string'},
    {'field':'open_idfa', 'dtype':'string'},
    {'field':'network_technology', 'dtype':'string'},
    {'field':'network_type', 'dtype':'string'}
    ] %}

  {{ return(mobile_context_fields) }}

{% endmacro %}
```

</DbtDetails>

</DbtDetails>

### Screen Context Fields {#macro.snowplow_mobile.screen_context_fields}

<DbtDetails><summary>
<code>macros/bigquery/context_fields.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro screen_context_fields() %}

  {% set screen_context_fields = [
      {'field':('id', 'screen_id'), 'dtype':'string'},
      {'field':('name', 'screen_name'), 'dtype':'string'},
      {'field':('activity', 'screen_activity'), 'dtype':'string'},
      {'field':('fragment', 'screen_fragment'), 'dtype':'string'},
      {'field':('top_view_controller', 'screen_top_view_controller'), 'dtype':'string'},
      {'field':('type', 'screen_type'), 'dtype':'string'},
      {'field':('view_controller', 'screen_view_controller'), 'dtype':'string'}
    ] %}

  {{ return(screen_context_fields) }}

{% endmacro %}
```

</DbtDetails>

</DbtDetails>

### Screen View Event Fields {#macro.snowplow_mobile.screen_view_event_fields}

<DbtDetails><summary>
<code>macros/bigquery/unstruct_event_fields.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro screen_view_event_fields() %}
  
  {% set screen_view_event_fields = [
    {'field':('id', 'screen_view_id'), 'dtype':'string'},
    {'field':('name', 'screen_view_name'), 'dtype':'string'},
    {'field':('previous_id', 'screen_view_previous_id'), 'dtype':'string'},
    {'field':('previous_name', 'screen_view_previous_name'), 'dtype':'string'},
    {'field':('previous_type', 'screen_view_previous_type'), 'dtype':'string'},
    {'field':('transition_type', 'screen_view_transition_type'), 'dtype':'string'},
    {'field':('type', 'screen_view_type'), 'dtype':'string'}
    ] %}

  {{ return(screen_view_event_fields) }}

{% endmacro %}
```

</DbtDetails>

</DbtDetails>

### Session Context Fields {#macro.snowplow_mobile.session_context_fields}

<DbtDetails><summary>
<code>macros/bigquery/context_fields.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro session_context_fields() %}

  {% set session_context_fields = [
    {'field':'session_id', 'dtype':'string'},
    {'field':'session_index', 'dtype':'integer'},
    {'field':'previous_session_id', 'dtype':'string'},
    {'field':('user_id', 'device_user_id'), 'dtype':'string'},
    {'field':('first_event_id', 'session_first_event_id'), 'dtype':'string'}
    ] %}

  {{ return(session_context_fields) }}

{% endmacro %}
```

</DbtDetails>

</DbtDetails>

### Stitch User Identifiers {#macro.snowplow_mobile.stitch_user_identifiers}

<DbtDetails><summary>
<code>macros/stitch_user_identifiers.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro stitch_user_identifiers(enabled, relation=this, user_mapping_relation=ref('snowplow_mobile_user_mapping')) %}

  {% if enabled and target.type not in ['databricks', 'spark'] | as_bool() %}

    -- Update sessions table with mapping
    update {{ relation }} as s
    set stitched_user_id = um.user_id
    from {{ user_mapping_relation }} as um
    where s.device_user_id = um.device_user_id;

{% elif enabled and target.type in ['databricks', 'spark']  | as_bool() %}

    -- Update sessions table with mapping
    merge into {{ relation }} as s
    using {{ user_mapping_relation }} as um
    on s.device_user_id = um.device_user_id
    when matched then 
      update set s.stitched_user_id = um.user_id;

  {% endif %}

{% endmacro %}
```

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="model" label="Models" default>

- [model.snowplow_mobile.snowplow_mobile_sessions](#model.snowplow_mobile.snowplow_mobile_sessions)

</TabItem>
</Tabs>
</DbtDetails>

### Snowplow Mobile Delete From Manifest {#macro.snowplow_utils.snowplow_mobile_delete_from_manifest}

<DbtDetails><summary>
<code>macros/utils/snowplow_delete_from_manifest.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro snowplow_mobile_delete_from_manifest(models) %}

  {{ snowplow_utils.snowplow_delete_from_manifest(models, ref('snowplow_mobile_incremental_manifest')) }}

{% endmacro %}
```

</DbtDetails>

</DbtDetails>


## Snowplow Media Player
### Get Percentage Boundaries {#macro.snowplow_media_player.get_percentage_boundaries}

<DbtDetails><summary>
<code>macros/get_percentage_boundaries.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

```jinja2
{% macro get_percentage_boundaries(tracked_boundaries) %}

   {% set percentage_boundaries = [] %}

   {% for element in var("snowplow__percent_progress_boundaries") %}
     {% if element < 0 or element > 100 %}
       {{ exceptions.raise_compiler_error("`snowplow__percent_progress_boundary` is outside the accepted range 0-100. Got: " ~ element) }}

     {% elif element % 1 != 0 %}
       {{ exceptions.raise_compiler_error("`snowplow__percent_progress_boundary` needs to be a whole number. Got: " ~ element) }}

     {% else %}
       {% do percentage_boundaries.append(element) %}
     {% endif %}
   {% endfor %}

   {% if 100 not in var("snowplow__percent_progress_boundaries") %}
     {% do percentage_boundaries.append('100') %}
   {% endif %}

   {{ return(percentage_boundaries) }}

 {% endmacro %}
```

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="model" label="Models" default>

- [model.snowplow_media_player.snowplow_media_player_pivot_base](#model.snowplow_media_player.snowplow_media_player_pivot_base)
- [model.snowplow_media_player.snowplow_media_player_media_stats](#model.snowplow_media_player.snowplow_media_player_media_stats)

</TabItem>
</Tabs>
</DbtDetails>


## Snowplow Normalize
### Allow Refresh {#macro.snowplow_normalize.allow_refresh}

<DbtDetails><summary>
<code>macros/allow_refresh.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

<Tabs groupId="dispatched_sql">
<TabItem value="raw" label="Raw" default>

```jinja2
{% macro allow_refresh() %}
  {{ return(adapter.dispatch('allow_refresh', 'snowplow_normalize')()) }}
{% endmacro %}
```
</TabItem>
<TabItem value="default" label="default">

```jinja2
{% macro default__allow_refresh() %}

  {% set allow_refresh = snowplow_utils.get_value_by_target(
                                    dev_value=none,
                                    default_value=var('snowplow__allow_refresh'),
                                    dev_target_name=var('snowplow__dev_target_name')
                                    ) %}

  {{ return(allow_refresh) }}

{% endmacro %}
```
</TabItem>
</Tabs>

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="model" label="Models" default>

- [model.snowplow_normalize.snowplow_normalize_incremental_manifest](#model.snowplow_normalize.snowplow_normalize_incremental_manifest)

</TabItem>
</Tabs>
</DbtDetails>

### Normalize Events {#macro.snowplow_normalize.normalize_events}

<DbtDetails><summary>
<code>macros/normalize_events.sql</code>
</summary>

#### Description
A macro to produce a table from `base_events_this_run` with the input columns, for a single event type

#### Arguments
- `event_names` *(array)*: List of names of the events this table will be filtered to
- `flat_cols` *(array)*: List of standard columns from the atomic.events table to include
- `sde_cols` *(string)*: Column names for the self-describing event to pull attributes from
- `sde_keys` *(array)*: List of list of  keys/column names within the self describing event column to include
- `sde_types` *(array)*: List of list of types of the values of the keys within the self describing event column (only used in Snowflake)
- `sde_aliases` *(array)*: List of prefixes to apply to the respective context column keys to be used as final column names
- `context_cols` *(array)*: List of context columns from the atomic.events table to include
- `context_keys` *(array of arrays)*: List of lists of keys/column names within the respective context column to include
- `context_types` *(array of arrays)*: List of list of types of the values of the keys within the respective context column (only used in Snowflake)
- `context_aliases` *(array)*: List of prefixes to apply to the respective context column keys to be used as final column names
- `remove_new_event_check` *(boolean)*: A flag to disable the `with_new_events` part of the macro, to allow for integration tests to run

#### Details
<DbtDetails>
<summary>Code</summary>

<Tabs groupId="dispatched_sql">
<TabItem value="raw" label="Raw" default>

```jinja2
{% macro normalize_events(event_names, flat_cols = [], sde_cols = [], sde_keys = [], sde_types = [], sde_aliases = [], context_cols = [], context_keys = [], context_types = [], context_aliases = [], remove_new_event_check = false) %}
    {{ return(adapter.dispatch('normalize_events', 'snowplow_normalize')(event_names, flat_cols, sde_cols, sde_keys, sde_types, sde_aliases, context_cols, context_keys, context_types, context_aliases, remove_new_event_check)) }}
{% endmacro %}
```
</TabItem>
<TabItem value="snowflake" label="snowflake">

```jinja2
{% macro snowflake__normalize_events(event_names, flat_cols = [], sde_cols = [], sde_keys = [], sde_types = [], sde_aliases = [], context_cols = [], context_keys = [], context_types = [], context_aliases = [], remove_new_event_check = false) %}
{# Remove down to major version for Snowflake columns, drop 2 last _X values #}
{%- set sde_cols_clean = [] -%}
{%- for ind in range(sde_cols|length) -%}
    {% do sde_cols_clean.append('_'.join(sde_cols[ind].split('_')[:-2])) -%}
{%- endfor -%}

{%- set context_cols_clean = [] -%}
{%- for ind in range(context_cols|length) -%}
    {% do context_cols_clean.append('_'.join(context_cols[ind].split('_')[:-2])) -%}
{%- endfor -%}

select
    event_id
    , collector_tstamp
    -- Flat columns from event table
    {% if flat_cols|length > 0 %}
        {%- for col in flat_cols -%}
            , {{ col }}
        {% endfor -%}
    {%- endif -%}
    -- self describing events columns from event table
    {% if sde_cols_clean|length > 0 %}
        {%- for col, col_ind in zip(sde_cols_clean, range(sde_cols_clean|length)) -%} {# Loop over each sde column #}
            {%- for key, type in zip(sde_keys[col_ind], sde_types[col_ind]) -%} {# Loop over each key within the sde column #}
                {% if sde_aliases|length > 0 -%}
                    , {{ col }}:{{ key }}::{{ type }} as {{ sde_aliases[col_ind] }}_{{ snowplow_normalize.snakeify_case(key) }} {# Alias should align across all warehouses in snakecase #}
                {% else -%}
                    , {{ col }}:{{ key }}::{{ type }} as {{ snowplow_normalize.snakeify_case(key) }}
                {% endif -%}
            {%- endfor -%}
        {%- endfor -%}
    {%- endif %}
    -- context column(s) from the event table
    {% if context_cols_clean|length > 0 %}
        {%- for col, col_ind in zip(context_cols_clean, range(context_cols_clean|length)) -%} {# Loop over each context column #}
            {%- for key, type in zip(context_keys[col_ind], context_types[col_ind]) -%} {# Loop over each key within the context column #}
                {% if context_aliases|length > 0 -%}
                    , {{ col }}[0]:{{ key }}::{{ type }} as {{ context_aliases[col_ind] }}_{{ snowplow_normalize.snakeify_case(key) }} {# Alias should align across all warehouses in snakecase #}
                {% else -%}
                    , {{ col }}[0]:{{ key }}::{{ type }} as {{ snowplow_normalize.snakeify_case(key) }}
                {% endif -%}
            {%- endfor -%}
        {%- endfor -%}
    {%- endif %}
from
    {{ ref('snowplow_normalize_base_events_this_run') }}
where
    event_name in ('{{ event_names|join("','") }}')
    {% if not remove_new_event_check %}
        and {{ snowplow_utils.is_run_with_new_events("snowplow_normalize") }}
    {%- endif -%}
{% endmacro %}
```
</TabItem>
<TabItem value="bigquery" label="bigquery">

```jinja2
{% macro bigquery__normalize_events(event_names, flat_cols = [], sde_cols = [], sde_keys = [], sde_types = [], sde_aliases = [], context_cols = [], context_keys = [], context_types = [], context_aliases = [], remove_new_event_check = false) %}
{# Remove down to major version for bigquery combine columns macro, drop 2 last _X values #}
{%- set sde_cols_clean = [] -%}
{%- for ind in range(sde_cols|length) -%}
    {% do sde_cols_clean.append('_'.join(sde_cols[ind].split('_')[:-2])) -%}
{%- endfor -%}
{%- set context_cols_clean = [] -%}
{%- for ind in range(context_cols|length) -%}
    {% do context_cols_clean.append('_'.join(context_cols[ind].split('_')[:-2])) -%}
{%- endfor -%}

{# Replace keys with snake_case where needed #}
{%- set sde_keys_clean = [] -%}
{%- set context_keys_clean = [] -%}

{%- for ind1 in range(sde_keys|length) -%}
    {%- set sde_key_clean = [] -%}
    {%- for ind2 in range(sde_keys[ind1]|length) -%}
        {% do sde_key_clean.append(snowplow_normalize.snakeify_case(sde_keys[ind1][ind2])) -%}
    {%- endfor -%}
    {% do sde_keys_clean.append(sde_key_clean) -%}
{%- endfor -%}

{%- for ind1 in range(context_keys|length) -%}
    {%- set context_key_clean = [] -%}
    {%- for ind2 in range(context_keys[ind1]|length) -%}
        {% do context_key_clean.append(snowplow_normalize.snakeify_case(context_keys[ind1][ind2])) -%}
    {%- endfor -%}
    {% do context_keys_clean.append(context_key_clean) -%}
{%- endfor -%}



select
    event_id
    , collector_tstamp
    -- Flat columns from event table
    {% if flat_cols|length > 0 %}
        {%- for col in flat_cols -%}
            , {{ col }}
        {% endfor -%}
    {%- endif -%}
    -- self describing events columns from event table
    {% if sde_cols|length > 0 %}
        {%- for col, col_ind in zip(sde_cols_clean, range(sde_cols|length)) -%} {# Loop over each sde column, get coalesced version of keys #}
            {%- set sde_col_list = snowplow_utils.combine_column_versions(
                                        relation=ref('snowplow_normalize_base_events_this_run'),
                                        column_prefix=col.lower(),
                                        include_field_alias = False,
                                        required_fields = sde_keys_clean[col_ind]
                                        ) -%}
            {% for field, key_ind in zip(sde_col_list, range(sde_col_list|length)) %} {# Loop over each key within the column, appling the bespoke alias as needed #}
                , {{field}}
                {%- if sde_aliases|length > 0 -%} {# The following contains a very cursed hack to ensure there is a space before the as, as I can't promise it ends up on a newline  #}
                    {# #} as {{ sde_aliases[col_ind] }}_{{ sde_keys_clean[col_ind][key_ind] }}
                {%- else -%}
                    {# #} as {{ sde_keys_clean[col_ind][key_ind] }}
                {%- endif -%}
            {% endfor -%}
        {%- endfor -%}
    {%- endif %}
    -- context column(s) from the event table
    {% if context_cols|length > 0 %}
        {%- for col, col_ind in zip(context_cols_clean, range(context_cols|length)) -%} {# Loop over each context column, get coalesced version of keys #}
            {%- set cont_col_list = snowplow_utils.combine_column_versions(
                                        relation=ref('snowplow_normalize_base_events_this_run'),
                                        column_prefix=col.lower(),
                                        include_field_alias = False,
                                        required_fields = context_keys_clean[col_ind]
                                        ) -%}
            {% for field, key_ind in zip(cont_col_list, range(cont_col_list|length)) %} {# Loop over each key within the column, appling the bespoke alias as needed #}
                , {{field}}
                {%- if context_aliases|length > 0 -%} {# The following contains a very cursed hack to ensure there is a space before the as, as I can't promise it ends up on a newline #}
                    {# #} as {{ context_aliases[col_ind] }}_{{ context_keys_clean[col_ind][key_ind] }}
                {%- else -%}
                    {# #} as {{ context_keys_clean[col_ind][key_ind] }}
                {%- endif -%}
            {% endfor -%}
        {%- endfor -%}
    {%- endif %}
from
    {{ ref('snowplow_normalize_base_events_this_run') }}
where
    event_name in ('{{ event_names|join("','") }}')
    {% if not remove_new_event_check %}
        and {{ snowplow_utils.is_run_with_new_events("snowplow_normalize") }}
    {%- endif -%}
{% endmacro %}
```
</TabItem>
<TabItem value="databricks" label="databricks">

```jinja2
{% macro databricks__normalize_events(event_names, flat_cols = [], sde_cols = [], sde_keys = [], sde_types = [], sde_aliases = [], context_cols = [], context_keys = [], context_types = [], context_aliases = [], remove_new_event_check = false) %}
{# Remove down to major version for Databricks columns, drop 2 last _X values #}
{%- set sde_cols_clean = [] -%}
{%- for ind in range(sde_cols|length) -%}
    {% do sde_cols_clean.append('_'.join(sde_cols[ind].split('_')[:-2])) -%}
{%- endfor -%}

{%- set context_cols_clean = [] -%}
{%- for ind in range(context_cols|length) -%}
    {% do context_cols_clean.append('_'.join(context_cols[ind].split('_')[:-2])) -%}
{%- endfor -%}

{# Replace keys with snake_case where needed #}
{%- set sde_keys_clean = [] -%}
{%- set context_keys_clean = [] -%}

{%- for ind1 in range(sde_keys|length) -%}
    {%- set sde_key_clean = [] -%}
    {%- for ind2 in range(sde_keys[ind1]|length) -%}
        {% do sde_key_clean.append(snowplow_normalize.snakeify_case(sde_keys[ind1][ind2])) -%}
    {%- endfor -%}
    {% do sde_keys_clean.append(sde_key_clean) -%}
{%- endfor -%}

{%- for ind1 in range(context_keys|length) -%}
    {%- set context_key_clean = [] -%}
    {%- for ind2 in range(context_keys[ind1]|length) -%}
        {% do context_key_clean.append(snowplow_normalize.snakeify_case(context_keys[ind1][ind2])) -%}
    {%- endfor -%}
    {% do context_keys_clean.append(context_key_clean) -%}
{%- endfor -%}

select
    event_id
    , collector_tstamp
    {% if target.type in ['databricks', 'spark'] -%}
    , DATE(collector_tstamp) as collector_tstamp_date
    {%- endif %}
    -- Flat columns from event table
    {% if flat_cols|length > 0 %}
    {%- for col in flat_cols -%}
    , {{ col }}
    {% endfor -%}
    {%- endif -%}
    -- self describing events columns from event table
    {% if sde_cols_clean|length > 0 %}
        {%- for col, col_ind in zip(sde_cols_clean, range(sde_cols_clean|length)) -%} {# Loop over each sde column #}
            {%- for key in sde_keys_clean[col_ind] -%} {# Loop over each key within the sde column #}
                {% if sde_aliases|length > 0 -%}
                    , {{ col }}.{{ key }} as {{ sde_aliases[col_ind] }}_{{ key }}
                {% else -%}
                    , {{ col }}.{{ key }} as {{ key }}
                {% endif -%}
            {%- endfor -%}
        {%- endfor -%}
    {%- endif %}

    -- context column(s) from the event table
    {% if context_cols_clean|length > 0 %}
        {%- for col, col_ind in zip(context_cols_clean, range(context_cols_clean|length)) -%} {# Loop over each context column #}
            {%- for key in context_keys_clean[col_ind] -%} {# Loop over each key within the context column #}
                {% if context_aliases|length > 0 -%}
                    , {{ col }}[0].{{ key }} as {{ context_aliases[col_ind] }}_{{ key }}
                {% else -%}
                    , {{ col }}[0].{{ key }} as {{ key }}
                {% endif -%}
            {%- endfor -%}
        {%- endfor -%}
    {%- endif %}
from
    {{ ref('snowplow_normalize_base_events_this_run') }}
where
    event_name in ('{{ event_names|join("','") }}')
    {% if not remove_new_event_check %}
        and {{ snowplow_utils.is_run_with_new_events("snowplow_normalize") }}
    {%- endif -%}
{% endmacro %}
```
</TabItem>
</Tabs>

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="macros" label="Macros">

- [macro.snowplow_normalize_integration_tests.bigquery__test_normalize_events](#macro.snowplow_normalize_integration_tests.bigquery__test_normalize_events)
- [macro.snowplow_normalize_integration_tests.snowflake__test_normalize_events](#macro.snowplow_normalize_integration_tests.snowflake__test_normalize_events)
- [macro.snowplow_normalize_integration_tests.databricks__test_normalize_events](#macro.snowplow_normalize_integration_tests.databricks__test_normalize_events)

</TabItem>
</Tabs>
</DbtDetails>

### Snakeify Case {#macro.snowplow_normalize.snakeify_case}

<DbtDetails><summary>
<code>macros/snakeify_case.sql</code>
</summary>

#### Description
Take a string in camel/pascal case and make it snakecase

#### Arguments
- `text` *(string)*: the text to convert to snakecase

#### Details
<DbtDetails>
<summary>Code</summary>

<Tabs groupId="dispatched_sql">
<TabItem value="raw" label="Raw" default>

```jinja2
{% macro snakeify_case(text) %}
    {{ return(adapter.dispatch('snakeify_case', 'snowplow_normalize')(text)) }}
{% endmacro %}
```
</TabItem>
<TabItem value="default" label="default">

```jinja2
{% macro default__snakeify_case(text) %}
    {%- set re = modules.re -%}
    {%- set camel_string1 = '([A-Z]+)([A-Z][a-z])'-%} {# Capitals followed by a lowercase  #}
    {%- set camel_string2 = '([a-z\d])([A-Z])'-%} {# lowercase followed by a capital #}
    {%- set replace_string = '\\1_\\2' -%}
    {%- set output_text = re.sub(camel_string2, replace_string, re.sub(camel_string1, replace_string, text)).replace('-', '_').lower() -%}
    {{- output_text -}}
{% endmacro %}
```
</TabItem>
</Tabs>

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="macros" label="Macros">

- [macro.snowplow_normalize.databricks__normalize_events](#macro.snowplow_normalize.databricks__normalize_events)
- [macro.snowplow_normalize.snowflake__normalize_events](#macro.snowplow_normalize.snowflake__normalize_events)
- [macro.snowplow_normalize.snowflake__users_table](#macro.snowplow_normalize.snowflake__users_table)
- [macro.snowplow_normalize_integration_tests.default__test_snakeify_case](#macro.snowplow_normalize_integration_tests.default__test_snakeify_case)
- [macro.snowplow_normalize.bigquery__normalize_events](#macro.snowplow_normalize.bigquery__normalize_events)
- [macro.snowplow_normalize.databricks__users_table](#macro.snowplow_normalize.databricks__users_table)
- [macro.snowplow_normalize.bigquery__users_table](#macro.snowplow_normalize.bigquery__users_table)

</TabItem>
</Tabs>
</DbtDetails>

### Users Table {#macro.snowplow_normalize.users_table}

<DbtDetails><summary>
<code>macros/users_table.sql</code>
</summary>

#### Description
A macro to produce a users table from the `base_events_this_run` table, using the latest context values as defined by the collector_tstamp.

#### Arguments
- `user_id_field` *(string)*: The name of the field to use as the unique user_id
- `user_id_sde` *(string)*: The name of the SDE column that contains the user_id_field
- `user_id_context` *(string)*: The name of the context column that contains the user_id_field, not used if user_id_sde is also provided
- `user_cols` *(array)*: List of (user related) context columns from the atomic.events table to include
- `user_keys` *(array of arrays)*: List of lists of keys/column names within the respective user context column to include
- `user_types` *(array of arrays)*: List of list of types of the values of the keys within the respective user context column (only used in Snowflake)
- `remove_new_event_check` *(boolean)*: A flag to disable the `with_new_events` part of the macro, to allow for integration tests to run

#### Details
<DbtDetails>
<summary>Code</summary>

<Tabs groupId="dispatched_sql">
<TabItem value="raw" label="Raw" default>

```jinja2
{% macro users_table(user_id_field = 'user_id', user_id_sde = '', user_id_context = '', user_cols = [], user_keys = [], user_types = [], remove_new_event_check = false) %}
    {{ return(adapter.dispatch('users_table', 'snowplow_normalize')(user_id_field, user_id_sde, user_id_context, user_cols, user_keys, user_types, remove_new_event_check)) }}
{% endmacro %}
```
</TabItem>
<TabItem value="snowflake" label="snowflake">

```jinja2
{% macro snowflake__users_table(user_id_field = 'user_id', user_id_sde = '', user_id_context = '', user_cols = [], user_keys = [], user_types = [], remove_new_event_check = false) %}
{# Remove down to major version for Snowflake columns, drop 2 last _X values #}
{%- set user_cols_clean = [] -%}
{%- for ind in range(user_cols|length) -%}
    {% do user_cols_clean.append('_'.join(user_cols[ind].split('_')[:-2])) -%}
{%- endfor -%}

{# Raise a warining if both sde and context are provided as we only use one #}
{%- if user_id_sde != '' and user_id_context != '' -%}
{% do exceptions.warn("Snowplow: Both a user_id sde column and context column provided, only the sde column will be used.") %}
{%- endif -%}

with defined_user_id as (
select
    {% if user_id_sde == '' and user_id_context == '' %}
        {{snowplow_normalize.snakeify_case(user_id_field)}} as user_id {# Snakeify case of standard column even in snowflake #}
    {% elif user_id_sde != '' %}
        {{ '_'.join(user_id_sde.split('_')[:-2]) }}:{{user_id_field}}::string as user_id
    {% elif user_id_context != '' %}
        {{ '_'.join(user_id_context.split('_')[:-2]) }}[0]:{{user_id_field}}::string as user_id
    {%- endif %}
    , collector_tstamp as latest_collector_tstamp
    -- user column(s) from the event table
    {% if user_cols_clean|length > 0 %}
        {%- for col, col_ind in zip(user_cols_clean, range(user_cols_clean|length)) -%} {# Loop over each context column provided #}
            {%- for key, type in zip(user_keys[col_ind], user_types[col_ind]) -%} {# Loop over the keys in each column #}
                , {{ col }}[0]:{{ key }}::{{ type }} as {{ snowplow_normalize.snakeify_case(key) }}
            {% endfor -%}
        {%- endfor -%}
    {%- endif %}
from
    {{ ref('snowplow_normalize_base_events_this_run') }}
where
    1 = 1
    {% if not remove_new_event_check %}
        and {{ snowplow_utils.is_run_with_new_events("snowplow_normalize") }}
    {%- endif -%}
)

{# Ensure only latest record is upserted into the table #}
select
    *
from
    defined_user_id
where
    user_id is not null
qualify
    row_number() over (partition by user_id order by latest_collector_tstamp desc) = 1
{% endmacro %}
```
</TabItem>
<TabItem value="bigquery" label="bigquery">

```jinja2
{% macro bigquery__users_table(user_id_field = 'user_id', user_id_sde = '', user_id_context = '', user_cols = [], user_keys = [], user_types = [], remove_new_event_check = false) %}
{# Remove down to major version for bigquery combine columns macro, drop 2 last _X values #}
{%- set user_cols_clean = [] -%}
{%- for ind in range(user_cols|length) -%}
    {% do user_cols_clean.append('_'.join(user_cols[ind].split('_')[:-2])) -%}
{%- endfor -%}

{# Replace keys with snake_case where needed #}
{%- set user_keys_clean = [] -%}
{%- for ind1 in range(user_keys|length) -%}
    {%- set user_key_clean = [] -%}
    {%- for ind2 in range(user_keys[ind1]|length) -%}
        {% do user_key_clean.append(snowplow_normalize.snakeify_case(user_keys[ind1][ind2])) -%}
    {%- endfor -%}
    {% do user_keys_clean.append(user_key_clean) -%}
{%- endfor -%}
{% set user_id_field = snowplow_normalize.snakeify_case(user_id_field) %}

{# Raise a warining if both sde and context are provided as we only use one #}
{%- if user_id_sde != '' and user_id_context != '' -%}
{% do exceptions.warn("Snowplow: Both a user_id sde column and context column provided, only the sde column will be used.") %}
{%- endif -%}



with defined_user_id as (
    select
        {% if user_id_sde == '' and user_id_context == ''%}
            {{snowplow_normalize.snakeify_case(user_id_field)}} as user_id
        {% elif user_id_sde != '' %}
        {# Coalesce the sde column for the custom user_id field  #}
            {%- set user_id_sde_coal = snowplow_utils.combine_column_versions(
                                        relation=ref('snowplow_normalize_base_events_this_run'),
                                        column_prefix= user_id_sde.lower(),
                                        include_field_alias = False,
                                        required_fields = [ user_id_field ]
                                        ) -%}
            {{ user_id_sde_coal[0] }} as user_id

        {% elif user_id_context != '' %}
        {# Coalesce the context column for the custom user_id field  #}
            {%- set user_id_cont_coal = snowplow_utils.combine_column_versions(
                                        relation=ref('snowplow_normalize_base_events_this_run'),
                                        column_prefix= user_id_context.lower(),
                                        include_field_alias = False,
                                        required_fields = [ user_id_field ]
                                        ) -%}
            {{ user_id_cont_coal[0] }} as user_id
        {%- endif %}
        , collector_tstamp as latest_collector_tstamp
        -- user column(s) from the event table
        {% if user_cols|length > 0 %}
            {%- for col, col_ind in zip(user_cols_clean, range(user_cols|length)) -%}  {# Loop over each context column, getting the coalesced version#}
                {%- set user_cols_list = snowplow_utils.combine_column_versions(
                                            relation=ref('snowplow_normalize_base_events_this_run'),
                                            column_prefix=col.lower(),
                                            include_field_alias = True,
                                            required_fields = user_keys_clean[col_ind]
                                            ) -%}
                {% for field in user_cols_list %} {# Loop over each field in the column, alias provided by macro #}
                    , {{field}}
                {%- endfor -%}
            {%- endfor -%}
        {%- endif %}
    from
        {{ ref('snowplow_normalize_base_events_this_run') }}
    where
        1 = 1
        {% if not remove_new_event_check %}
            and {{ snowplow_utils.is_run_with_new_events("snowplow_normalize") }}
        {%- endif -%}
),

{# Order data to get the latest data having rn = 1 #}
users_ordering as (
    select
        a.*
        , row_number() over (partition by user_id order by latest_collector_tstamp desc) as rn
    from
        defined_user_id a
    where
        user_id is not null
)

{# Ensure only latest record is upserted into the table #}
select
    * except (rn)
from
    users_ordering
where
    rn = 1
{% endmacro %}
```
</TabItem>
<TabItem value="databricks" label="databricks">

```jinja2
{% macro databricks__users_table(user_id_field = 'user_id', user_id_sde = '', user_id_context = '', user_cols = [], user_keys = [], user_types = [], remove_new_event_check = false) %}
{# Remove down to major version for Databricks columns, drop 2 last _X values #}
{%- set user_cols_clean = [] -%}
{%- for ind in range(user_cols|length) -%}
    {% do user_cols_clean.append('_'.join(user_cols[ind].split('_')[:-2])) -%}
{%- endfor -%}

{# Replace keys with snake_case where needed #}
{%- set user_keys_clean = [] -%}
{%- for ind1 in range(user_keys|length) -%}
    {%- set user_key_clean = [] -%}
    {%- for ind2 in range(user_keys[ind1]|length) -%}
        {% do user_key_clean.append(snowplow_normalize.snakeify_case(user_keys[ind1][ind2])) -%}
    {%- endfor -%}
    {% do user_keys_clean.append(user_key_clean) -%}
{%- endfor -%}
{% set user_id_field = snowplow_normalize.snakeify_case(user_id_field) %}

{# Raise a warining if both sde and context are provided as we only use one #}
{%- if user_id_sde != '' and user_id_context != '' -%}
{% do exceptions.warn("Snowplow: Both a user_id sde column and context column provided, only the sde column will be used.") %}
{%- endif -%}


with defined_user_id as (
    select
        {% if user_id_sde == '' and user_id_context == ''%}
            {{ user_id_field }} as user_id
        {% elif user_id_sde != '' %}
            {{ '_'.join(user_id_sde.split('_')[:-2]) }}.{{ user_id_field }} as user_id
        {% elif user_id_context != '' %}
            {{ '_'.join(user_id_context.split('_')[:-2]) }}[0].{{ user_id_field }} as user_id
        {%- endif %}
        , collector_tstamp as latest_collector_tstamp
        {% if target.type in ['databricks', 'spark'] -%}
            , DATE(collector_tstamp) as latest_collector_tstamp_date
        {%- endif %}
        -- user column(s) from the event table
        {% if user_cols_clean|length > 0 %}
            {%- for col, col_ind in zip(user_cols_clean, range(user_cols_clean|length)) -%} {# Loop over each context column provided #}
                {%- for key in user_keys_clean[col_ind] -%} {# Loop over the keys in each column #}
                    , {{ col }}[0].{{ key }} as {{ key }}
                {% endfor -%}
            {%- endfor -%}
        {%- endif %}
    from
        {{ ref('snowplow_normalize_base_events_this_run') }}
    where
        1 = 1
        {% if not remove_new_event_check %}
            and {{ snowplow_utils.is_run_with_new_events("snowplow_normalize") }}
        {%- endif -%}

),

{# Order data to get the latest data having rn = 1 #}
users_ordering as (
select
    a.*
    , row_number() over (partition by user_id order by latest_collector_tstamp desc) as rn
from
    defined_user_id a
where
    user_id is not null
)

{# Ensure only latest record is upserted into the table #}
select
    * except (rn)
from
    users_ordering
where
    rn = 1
{% endmacro %}
```
</TabItem>
</Tabs>

</DbtDetails>


#### Referenced By
<Tabs groupId="reference">
<TabItem value="macros" label="Macros">

- [macro.snowplow_normalize_integration_tests.snowflake__test_users_table](#macro.snowplow_normalize_integration_tests.snowflake__test_users_table)
- [macro.snowplow_normalize_integration_tests.bigquery__test_users_table](#macro.snowplow_normalize_integration_tests.bigquery__test_users_table)
- [macro.snowplow_normalize_integration_tests.databricks__test_users_table](#macro.snowplow_normalize_integration_tests.databricks__test_users_table)

</TabItem>
</Tabs>
</DbtDetails>

### Test Normalize Events {#macro.snowplow_normalize_integration_tests.test_normalize_events}

<DbtDetails><summary>
<code>macros/test_normalize_events.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

<Tabs groupId="dispatched_sql">
<TabItem value="raw" label="Raw" default>

```jinja2
{% macro test_normalize_events() %}

    {{ return(adapter.dispatch('test_normalize_events', 'snowplow_normalize_integration_tests')()) }}

{% endmacro %}
```
</TabItem>
<TabItem value="snowflake" label="snowflake">

```jinja2
{% macro snowflake__test_normalize_events() %}

    {% set expected_dict = {
        "flat_cols_only" : "select event_id , collector_tstamp -- Flat columns from event table , app_id -- self describing events columns from event table -- context column(s) from the event table from "~target.database~"."~target.schema~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "sde_plus_cols" : "select event_id , collector_tstamp -- Flat columns from event table , app_id -- self describing events columns from event table , UNSTRUCT_EVENT_TEST_1:testId::string as test_id , UNSTRUCT_EVENT_TEST_1:testClass::boolean as test_class -- context column(s) from the event table from "~target.database~"."~target.schema~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "sde_plus_1_context" : "select event_id , collector_tstamp -- Flat columns from event table , app_id -- self describing events columns from event table , UNSTRUCT_EVENT_TEST_1:testId::string as test_id , UNSTRUCT_EVENT_TEST_1:testClass::boolean as test_class -- context column(s) from the event table , CONTEXTS_TEST_1[0]:contextTestId::string as context_test_id , CONTEXTS_TEST_1[0]:contextTestClass::integer as context_test_class from "~target.database~"."~target.schema~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "sde_plus_2_context" : "select event_id , collector_tstamp -- Flat columns from event table , app_id -- self describing events columns from event table , UNSTRUCT_EVENT_TEST_1:testId::string as test_id , UNSTRUCT_EVENT_TEST_1:testClass::boolean as test_class -- context column(s) from the event table , CONTEXTS_TEST_1[0]:contextTestId::boolean as context_test_id , CONTEXTS_TEST_1[0]:contextTestClass::string as context_test_class , CONTEXTS_TEST2_1[0]:contextTestId2::interger as context_test_id2 , CONTEXTS_TEST2_1[0]:contextTestClass2::string as context_test_class2 from "~target.database~"."~target.schema~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "sde_plus_2_context_w_alias" : "select event_id , collector_tstamp -- Flat columns from event table , app_id -- self describing events columns from event table , UNSTRUCT_EVENT_TEST_1:testId::string as test_id , UNSTRUCT_EVENT_TEST_1:testClass::boolean as test_class -- context column(s) from the event table , CONTEXTS_TEST_1[0]:contextTestId::boolean as test1_context_test_id , CONTEXTS_TEST_1[0]:contextTestClass::string as test1_context_test_class , CONTEXTS_TEST2_1[0]:contextTestId2::interger as test2_context_test_id2 , CONTEXTS_TEST2_1[0]:contextTestClass2::string as test2_context_test_class2 from "~target.database~"."~target.schema~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "context_only" : "select event_id , collector_tstamp -- Flat columns from event table , app_id -- self describing events columns from event table -- context column(s) from the event table , CONTEXTS_TEST_1[0]:contextTestId::boolean as context_test_id , CONTEXTS_TEST_1[0]:contextTestClass::string as context_test_class , CONTEXTS_TEST2_1[0]:contextTestId2::interger as context_test_id2 , CONTEXTS_TEST2_1[0]:contextTestClass2::string as context_test_class2 from "~target.database~"."~target.schema~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "multiple_base_events" : "select event_id , collector_tstamp -- Flat columns from event table , app_id -- self describing events columns from event table -- context column(s) from the event table , CONTEXTS_TEST_1[0]:contextTestId::boolean as context_test_id , CONTEXTS_TEST_1[0]:contextTestClass::string as context_test_class , CONTEXTS_TEST2_1[0]:contextTestId2::interger as context_test_id2 , CONTEXTS_TEST2_1[0]:contextTestClass2::string as context_test_class2 from "~target.database~"."~target.schema~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name','page_ping')",
        "multiple_sde_events" : "select event_id , collector_tstamp -- Flat columns from event table , app_id -- self describing events columns from event table , UNSTRUCT_EVENT_TEST_1:testId::number as test1_test_id , UNSTRUCT_EVENT_TEST_1:testClass::string as test1_test_class -- context column(s) from the event table , CONTEXTS_TEST_1[0]:contextTestId::boolean as context_test_id , CONTEXTS_TEST_1[0]:contextTestClass::string as context_test_class , CONTEXTS_TEST2_1[0]:contextTestId2::interger as context_test_id2 , CONTEXTS_TEST2_1[0]:contextTestClass2::string as context_test_class2 from "~target.database~"."~target.schema~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')"
    } %}

   {% set results_dict ={
        "flat_cols_only" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], [], [], [], [], [], [], [], [], true).split()|join(' '),
        "sde_plus_cols" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], ['UNSTRUCT_EVENT_TEST_1_0_1'], [['testId', 'testClass']], [['string', 'boolean']], [], [], [], [], [], true).split()|join(' '),
        "sde_plus_1_context" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], ['UNSTRUCT_EVENT_TEST_1_0_1'], [['testId', 'testClass']], [['string', 'boolean']], [], ['CONTEXTS_TEST_1_0_0'], [['contextTestId', 'contextTestClass']], [['string', 'integer']], [], true).split()|join(' '),
        "sde_plus_2_context" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], ['UNSTRUCT_EVENT_TEST_1_0_1'], [['testId', 'testClass']], [['string', 'boolean']], [], ['CONTEXTS_TEST_1_0_0', 'CONTEXTS_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'], ['contextTestId2', 'contextTestClass2']], [['boolean', 'string'], ['interger', 'string']], [], true).split()|join(' '),
        "sde_plus_2_context_w_alias" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], ['UNSTRUCT_EVENT_TEST_1_0_1'], [['testId', 'testClass']], [['string', 'boolean']], [], ['CONTEXTS_TEST_1_0_0', 'CONTEXTS_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'],['contextTestId2', 'contextTestClass2'] ], [['boolean', 'string'], ['interger', 'string']], ['test1', 'test2'], true).split()|join(' '),
        "context_only" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], [], [], [], [], ['CONTEXTS_TEST_1_0_0', 'CONTEXTS_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'],['contextTestId2', 'contextTestClass2'] ], [['boolean', 'string'], ['interger', 'string']], [], true).split()|join(' '),
        "multiple_base_events" : snowplow_normalize.normalize_events(['event_name', 'page_ping'], ['app_id'], [], [], [], [], ['CONTEXTS_TEST_1_0_0', 'CONTEXTS_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'],['contextTestId2', 'contextTestClass2'] ], [['boolean', 'string'], ['interger', 'string']], [], true).split()|join(' '),
        "multiple_sde_events" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], ['UNSTRUCT_EVENT_TEST_1_0_1', 'UNSTRUCT_EVENT_TEST2_1_0_1'], [['testId', 'testClass'], ['testWord', 'testIdea']], [['number', 'string']], ['test1', 'test2'], ['CONTEXTS_TEST_1_0_0', 'CONTEXTS_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'],['contextTestId2', 'contextTestClass2'] ], [['boolean', 'string'], ['interger', 'string']], [], true).split()|join(' ')
        }
    %}


    {# {{ print(results_dict['flat_cols_only'])}} #}
    {# {{ print(results_dict['sde_plus_cols'])}} #}
    {# {{ print(results_dict['sde_plus_1_context'])}} #}
    {# {{ print(results_dict['sde_plus_2_context'])}} #}
    {# {{ print(results_dict['sde_plus_2_context_w_alias'])}} #}
    {# {{ print(results_dict['context_only'])}} #}
    {# {{ print(results_dict['multiple_base_events'])}} #}
    {# {{ print(results_dict['multiple_sde_events'])}} #}


    {{ dbt_unittest.assert_dict_equals(expected_dict, results_dict) }}


{% endmacro %}
```
</TabItem>
<TabItem value="bigquery" label="bigquery">

```jinja2
{% macro bigquery__test_normalize_events() %}

    {% set expected_dict = {
        "flat_cols_only" : "select event_id , collector_tstamp -- Flat columns from event table , app_id -- self describing events columns from event table -- context column(s) from the event table from `"~target.project~"`."~target.dataset~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "sde_plus_cols" : "select event_id , collector_tstamp -- Flat columns from event table , app_id -- self describing events columns from event table , coalesce(unstruct_event_test_1_0_1.test_id, unstruct_event_test_1_0_0.test_id) as test_id , coalesce(unstruct_event_test_1_0_1.test_class, unstruct_event_test_1_0_0.test_class) as test_class -- context column(s) from the event table from `"~target.project~"`."~target.dataset~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "sde_plus_1_context" : "select event_id , collector_tstamp -- Flat columns from event table , app_id -- self describing events columns from event table , coalesce(unstruct_event_test_1_0_1.test_id, unstruct_event_test_1_0_0.test_id) as test_id , coalesce(unstruct_event_test_1_0_1.test_class, unstruct_event_test_1_0_0.test_class) as test_class -- context column(s) from the event table , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_id) as context_test_id , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_class) as context_test_class from `"~target.project~"`."~target.dataset~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "sde_plus_2_context" : "select event_id , collector_tstamp -- Flat columns from event table , app_id -- self describing events columns from event table , coalesce(unstruct_event_test_1_0_1.test_id, unstruct_event_test_1_0_0.test_id) as test_id , coalesce(unstruct_event_test_1_0_1.test_class, unstruct_event_test_1_0_0.test_class) as test_class -- context column(s) from the event table , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_id) as context_test_id , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_class) as context_test_class , coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_id2, contexts_test2_1_0_4[safe_offset(0)].context_test_id2, contexts_test2_1_0_3[safe_offset(0)].context_test_id2, contexts_test2_1_0_2[safe_offset(0)].context_test_id2, contexts_test2_1_0_1[safe_offset(0)].context_test_id2, contexts_test2_1_0_0[safe_offset(0)].context_test_id2) as context_test_id2 , coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_class2, contexts_test2_1_0_4[safe_offset(0)].context_test_class2, contexts_test2_1_0_3[safe_offset(0)].context_test_class2, contexts_test2_1_0_2[safe_offset(0)].context_test_class2, contexts_test2_1_0_1[safe_offset(0)].context_test_class2, contexts_test2_1_0_0[safe_offset(0)].context_test_class2) as context_test_class2 from `"~target.project~"`."~target.dataset~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "sde_plus_2_context_w_alias" : "select event_id , collector_tstamp -- Flat columns from event table , app_id -- self describing events columns from event table , coalesce(unstruct_event_test_1_0_1.test_id, unstruct_event_test_1_0_0.test_id) as test_id , coalesce(unstruct_event_test_1_0_1.test_class, unstruct_event_test_1_0_0.test_class) as test_class -- context column(s) from the event table , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_id) as test1_context_test_id , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_class) as test1_context_test_class , coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_id2, contexts_test2_1_0_4[safe_offset(0)].context_test_id2, contexts_test2_1_0_3[safe_offset(0)].context_test_id2, contexts_test2_1_0_2[safe_offset(0)].context_test_id2, contexts_test2_1_0_1[safe_offset(0)].context_test_id2, contexts_test2_1_0_0[safe_offset(0)].context_test_id2) as test2_context_test_id2 , coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_class2, contexts_test2_1_0_4[safe_offset(0)].context_test_class2, contexts_test2_1_0_3[safe_offset(0)].context_test_class2, contexts_test2_1_0_2[safe_offset(0)].context_test_class2, contexts_test2_1_0_1[safe_offset(0)].context_test_class2, contexts_test2_1_0_0[safe_offset(0)].context_test_class2) as test2_context_test_class2 from `"~target.project~"`."~target.dataset~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "context_only" : "select event_id , collector_tstamp -- Flat columns from event table , app_id -- self describing events columns from event table -- context column(s) from the event table , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_id) as context_test_id , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_class) as context_test_class , coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_id2, contexts_test2_1_0_4[safe_offset(0)].context_test_id2, contexts_test2_1_0_3[safe_offset(0)].context_test_id2, contexts_test2_1_0_2[safe_offset(0)].context_test_id2, contexts_test2_1_0_1[safe_offset(0)].context_test_id2, contexts_test2_1_0_0[safe_offset(0)].context_test_id2) as context_test_id2 , coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_class2, contexts_test2_1_0_4[safe_offset(0)].context_test_class2, contexts_test2_1_0_3[safe_offset(0)].context_test_class2, contexts_test2_1_0_2[safe_offset(0)].context_test_class2, contexts_test2_1_0_1[safe_offset(0)].context_test_class2, contexts_test2_1_0_0[safe_offset(0)].context_test_class2) as context_test_class2 from `"~target.project~"`."~target.dataset~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "multiple_base_events" : "select event_id , collector_tstamp -- Flat columns from event table , app_id -- self describing events columns from event table -- context column(s) from the event table , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_id) as context_test_id , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_class) as context_test_class , coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_id2, contexts_test2_1_0_4[safe_offset(0)].context_test_id2, contexts_test2_1_0_3[safe_offset(0)].context_test_id2, contexts_test2_1_0_2[safe_offset(0)].context_test_id2, contexts_test2_1_0_1[safe_offset(0)].context_test_id2, contexts_test2_1_0_0[safe_offset(0)].context_test_id2) as context_test_id2 , coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_class2, contexts_test2_1_0_4[safe_offset(0)].context_test_class2, contexts_test2_1_0_3[safe_offset(0)].context_test_class2, contexts_test2_1_0_2[safe_offset(0)].context_test_class2, contexts_test2_1_0_1[safe_offset(0)].context_test_class2, contexts_test2_1_0_0[safe_offset(0)].context_test_class2) as context_test_class2 from `"~target.project~"`."~target.dataset~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name','page_ping')",
        "multiple_sde_events" : "select event_id , collector_tstamp -- Flat columns from event table , app_id -- self describing events columns from event table , coalesce(unstruct_event_test_1_0_1.test_id, unstruct_event_test_1_0_0.test_id) as test1_test_id , coalesce(unstruct_event_test_1_0_1.test_class, unstruct_event_test_1_0_0.test_class) as test1_test_class , coalesce(unstruct_event_test2_1_0_1.test_word, unstruct_event_test2_1_0_0.test_word) as test2_test_word , coalesce(unstruct_event_test2_1_0_1.test_idea, unstruct_event_test2_1_0_0.test_idea) as test2_test_idea -- context column(s) from the event table , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_id) as context_test_id , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_class) as context_test_class , coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_id2, contexts_test2_1_0_4[safe_offset(0)].context_test_id2, contexts_test2_1_0_3[safe_offset(0)].context_test_id2, contexts_test2_1_0_2[safe_offset(0)].context_test_id2, contexts_test2_1_0_1[safe_offset(0)].context_test_id2, contexts_test2_1_0_0[safe_offset(0)].context_test_id2) as context_test_id2 , coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_class2, contexts_test2_1_0_4[safe_offset(0)].context_test_class2, contexts_test2_1_0_3[safe_offset(0)].context_test_class2, contexts_test2_1_0_2[safe_offset(0)].context_test_class2, contexts_test2_1_0_1[safe_offset(0)].context_test_class2, contexts_test2_1_0_0[safe_offset(0)].context_test_class2) as context_test_class2 from `"~target.project~"`."~target.dataset~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')"
    } %}



    {% set results_dict ={
        "flat_cols_only" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], [], [], [], [], [], [], [], [], true).split()|join(' '),
        "sde_plus_cols" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], ['UNSTRUCT_EVENT_TEST_1_0_1'], [['testId', 'testClass']], [['string', 'boolean']], [], [], [], [], [], true).split()|join(' '),
        "sde_plus_1_context" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], ['UNSTRUCT_EVENT_TEST_1_0_1'], [['testId', 'testClass']], [['string', 'boolean']], [], ['CONTEXTS_TEST_1_0_0'], [['contextTestId', 'contextTestClass']], [['string', 'integer']], [], true).split()|join(' '),
        "sde_plus_2_context" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], ['UNSTRUCT_EVENT_TEST_1_0_1'], [['testId', 'testClass']], [['string', 'boolean']], [], ['CONTEXTS_TEST_1_0_0', 'CONTEXTS_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'], ['contextTestId2', 'contextTestClass2']], [['boolean', 'string'], ['interger', 'string']], [], true).split()|join(' '),
        "sde_plus_2_context_w_alias" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], ['UNSTRUCT_EVENT_TEST_1_0_1'], [['testId', 'testClass']], [['string', 'boolean']], [], ['CONTEXTS_TEST_1_0_0', 'CONTEXTS_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'],['contextTestId2', 'contextTestClass2'] ], [['boolean', 'string'], ['interger', 'string']], ['test1', 'test2'], true).split()|join(' '),
        "context_only" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], [], [], [], [], ['CONTEXTS_TEST_1_0_0', 'CONTEXTS_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'],['contextTestId2', 'contextTestClass2'] ], [['boolean', 'string'], ['interger', 'string']], [], true).split()|join(' '),
        "multiple_base_events" : snowplow_normalize.normalize_events(['event_name', 'page_ping'], ['app_id'], [], [], [], [], ['CONTEXTS_TEST_1_0_0', 'CONTEXTS_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'],['contextTestId2', 'contextTestClass2'] ], [['boolean', 'string'], ['interger', 'string']], [], true).split()|join(' '),
        "multiple_sde_events" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], ['UNSTRUCT_EVENT_TEST_1_0_1', 'UNSTRUCT_EVENT_TEST2_1_0_1'], [['testId', 'testClass'], ['testWord', 'testIdea']], [['number', 'string']], ['test1', 'test2'], ['CONTEXTS_TEST_1_0_0', 'CONTEXTS_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'],['contextTestId2', 'contextTestClass2'] ], [['boolean', 'string'], ['interger', 'string']], [], true).split()|join(' ')
        }
    %}


    {# {{ print(results_dict['flat_cols_only'])}} #}
    {# {{ print(results_dict['sde_plus_cols'])}} #}
    {# {{ print(results_dict['sde_plus_1_context'])}} #}
    {# {{ print(results_dict['sde_plus_2_context'])}} #}
    {# {{ print(results_dict['sde_plus_2_context_w_alias'])}} #}
    {# {{ print(results_dict['context_only'])}} #}
    {# {{ print(results_dict['multiple_base_events'])}} #}
    {# {{ print(results_dict['multiple_sde_events'])}} #}


    {{ dbt_unittest.assert_dict_equals(expected_dict, results_dict) }}


{% endmacro %}
```
</TabItem>
<TabItem value="databricks" label="databricks">

```jinja2
{% macro databricks__test_normalize_events() %}

    {% set expected_dict = {
        "flat_cols_only" : "select event_id , collector_tstamp , DATE(collector_tstamp) as collector_tstamp_date -- Flat columns from event table , app_id -- self describing events columns from event table -- context column(s) from the event table from "~target.schema~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "sde_plus_cols" : "select event_id , collector_tstamp , DATE(collector_tstamp) as collector_tstamp_date -- Flat columns from event table , app_id -- self describing events columns from event table , UNSTRUCT_EVENT_TEST_1.test_id as test_id , UNSTRUCT_EVENT_TEST_1.test_class as test_class -- context column(s) from the event table from "~target.schema~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "sde_plus_1_context" : "select event_id , collector_tstamp , DATE(collector_tstamp) as collector_tstamp_date -- Flat columns from event table , app_id -- self describing events columns from event table , UNSTRUCT_EVENT_TEST_1.test_id as test_id , UNSTRUCT_EVENT_TEST_1.test_class as test_class -- context column(s) from the event table , CONTEXTS_TEST_1[0].context_test_id as context_test_id , CONTEXTS_TEST_1[0].context_test_class as context_test_class from "~target.schema~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "sde_plus_2_context" : "select event_id , collector_tstamp , DATE(collector_tstamp) as collector_tstamp_date -- Flat columns from event table , app_id -- self describing events columns from event table , UNSTRUCT_EVENT_TEST_1.test_id as test_id , UNSTRUCT_EVENT_TEST_1.test_class as test_class -- context column(s) from the event table , CONTEXTS_TEST_1[0].context_test_id as context_test_id , CONTEXTS_TEST_1[0].context_test_class as context_test_class , CONTEXTS_TEST2_1[0].context_test_id2 as context_test_id2 , CONTEXTS_TEST2_1[0].context_test_class2 as context_test_class2 from "~target.schema~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "sde_plus_2_context_w_alias" : "select event_id , collector_tstamp , DATE(collector_tstamp) as collector_tstamp_date -- Flat columns from event table , app_id -- self describing events columns from event table , UNSTRUCT_EVENT_TEST_1.test_id as test_id , UNSTRUCT_EVENT_TEST_1.test_class as test_class -- context column(s) from the event table , CONTEXTS_TEST_1[0].context_test_id as test1_context_test_id , CONTEXTS_TEST_1[0].context_test_class as test1_context_test_class , CONTEXTS_TEST2_1[0].context_test_id2 as test2_context_test_id2 , CONTEXTS_TEST2_1[0].context_test_class2 as test2_context_test_class2 from "~target.schema~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "context_only" : "select event_id , collector_tstamp , DATE(collector_tstamp) as collector_tstamp_date -- Flat columns from event table , app_id -- self describing events columns from event table -- context column(s) from the event table , CONTEXTS_TEST_1[0].context_test_id as context_test_id , CONTEXTS_TEST_1[0].context_test_class as context_test_class , CONTEXTS_TEST2_1[0].context_test_id2 as context_test_id2 , CONTEXTS_TEST2_1[0].context_test_class2 as context_test_class2 from "~target.schema~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')",
        "multiple_base_events" : "select event_id , collector_tstamp , DATE(collector_tstamp) as collector_tstamp_date -- Flat columns from event table , app_id -- self describing events columns from event table -- context column(s) from the event table , CONTEXTS_TEST_1[0].context_test_id as context_test_id , CONTEXTS_TEST_1[0].context_test_class as context_test_class , CONTEXTS_TEST2_1[0].context_test_id2 as context_test_id2 , CONTEXTS_TEST2_1[0].context_test_class2 as context_test_class2 from "~target.schema~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name','page_ping')",
        "multiple_sde_events" : "select event_id , collector_tstamp , DATE(collector_tstamp) as collector_tstamp_date -- Flat columns from event table , app_id -- self describing events columns from event table , UNSTRUCT_EVENT_TEST_1.test_id as test1_test_id , UNSTRUCT_EVENT_TEST_1.test_class as test1_test_class , UNSTRUCT_EVENT_TEST2_1.test_word as test2_test_word , UNSTRUCT_EVENT_TEST2_1.test_idea as test2_test_idea -- context column(s) from the event table , CONTEXTS_TEST_1[0].context_test_id as context_test_id , CONTEXTS_TEST_1[0].context_test_class as context_test_class , CONTEXTS_TEST2_1[0].context_test_id2 as context_test_id2 , CONTEXTS_TEST2_1[0].context_test_class2 as context_test_class2 from "~target.schema~"_scratch.snowplow_normalize_base_events_this_run where event_name in ('event_name')"
    } %}

    {% set results_dict ={
        "flat_cols_only" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], [], [], [], [], [], [], [], [], true).split()|join(' '),
        "sde_plus_cols" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], ['UNSTRUCT_EVENT_TEST_1_0_1'], [['testId', 'testClass']], [['string', 'boolean']], [], [], [], [], [], true).split()|join(' '),
        "sde_plus_1_context" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], ['UNSTRUCT_EVENT_TEST_1_0_1'], [['testId', 'testClass']], [['string', 'boolean']], [], ['CONTEXTS_TEST_1_0_0'], [['contextTestId', 'contextTestClass']], [['string', 'integer']], [], true).split()|join(' '),
        "sde_plus_2_context" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], ['UNSTRUCT_EVENT_TEST_1_0_1'], [['testId', 'testClass']], [['string', 'boolean']], [], ['CONTEXTS_TEST_1_0_0', 'CONTEXTS_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'], ['contextTestId2', 'contextTestClass2']], [['boolean', 'string'], ['interger', 'string']], [], true).split()|join(' '),
        "sde_plus_2_context_w_alias" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], ['UNSTRUCT_EVENT_TEST_1_0_1'], [['testId', 'testClass']], [['string', 'boolean']], [], ['CONTEXTS_TEST_1_0_0', 'CONTEXTS_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'],['contextTestId2', 'contextTestClass2'] ], [['boolean', 'string'], ['interger', 'string']], ['test1', 'test2'], true).split()|join(' '),
        "context_only" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], [], [], [], [], ['CONTEXTS_TEST_1_0_0', 'CONTEXTS_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'],['contextTestId2', 'contextTestClass2'] ], [['boolean', 'string'], ['interger', 'string']], [], true).split()|join(' '),
        "multiple_base_events" : snowplow_normalize.normalize_events(['event_name', 'page_ping'], ['app_id'], [], [], [], [], ['CONTEXTS_TEST_1_0_0', 'CONTEXTS_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'],['contextTestId2', 'contextTestClass2'] ], [['boolean', 'string'], ['interger', 'string']], [], true).split()|join(' '),
        "multiple_sde_events" : snowplow_normalize.normalize_events(['event_name'], ['app_id'], ['UNSTRUCT_EVENT_TEST_1_0_1', 'UNSTRUCT_EVENT_TEST2_1_0_1'], [['testId', 'testClass'], ['testWord', 'testIdea']], [['number', 'string']], ['test1', 'test2'], ['CONTEXTS_TEST_1_0_0', 'CONTEXTS_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'],['contextTestId2', 'contextTestClass2'] ], [['boolean', 'string'], ['interger', 'string']], [], true).split()|join(' ')
        }
    %}


    {# {{ print(results_dict['flat_cols_only'])}} #}
    {# {{ print(results_dict['sde_plus_cols'])}} #}
    {# {{ print(results_dict['sde_plus_1_context'])}} #}
    {# {{ print(results_dict['sde_plus_2_context'])}} #}
    {# {{ print(results_dict['sde_plus_2_context_w_alias'])}} #}
    {# {{ print(results_dict['context_only'])}} #}
    {# {{ print(results_dict['multiple_base_events'])}} #}
    {# {{ print(results_dict['multiple_sde_events'])}} #}


    {{ dbt_unittest.assert_dict_equals(expected_dict, results_dict) }}


{% endmacro %}
```
</TabItem>
</Tabs>

</DbtDetails>

</DbtDetails>

### Test Snakeify Case {#macro.snowplow_normalize_integration_tests.test_snakeify_case}

<DbtDetails><summary>
<code>macros/test_snakeify_case.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

<Tabs groupId="dispatched_sql">
<TabItem value="raw" label="Raw" default>

```jinja2
{% macro test_snakeify_case() %}

    {{ return(adapter.dispatch('test_snakeify_case', 'snowplow_normalize_integration_tests')()) }}

{% endmacro %}
```
</TabItem>
<TabItem value="default" label="default">

```jinja2
{% macro default__test_snakeify_case() %}

    {% set expected_dict = {
        "single_word" : "hello",
        "already_snake" : "hello_world",
        "pascal_case" : "hello_world",
        "camel_basic" : "hello_world",
        "camel_multi_word" : "hello_world_earth",
        "camel_end_with_caps" :"hello_world_earth",
        "camel_middle_caps" : "hello_world_earth",
        "camel_hyphens" : "hello_world_earth",
        "camel_numbers" : "hello_world23_earth"
    } %}

    {% set results_dict ={
        "single_word" : snowplow_normalize.snakeify_case('hello'),
        "already_snake" : snowplow_normalize.snakeify_case('hello_world'),
        "pascal_case" : snowplow_normalize.snakeify_case('HelloWorld'),
        "camel_basic" : snowplow_normalize.snakeify_case('helloWorld'),
        "camel_multi_word" : snowplow_normalize.snakeify_case('helloWorldEarth'),
        "camel_end_with_caps" : snowplow_normalize.snakeify_case('helloWorldEARTH'),
        "camel_middle_caps" : snowplow_normalize.snakeify_case('helloWORLDEarth'),
        "camel_middle_caps" : snowplow_normalize.snakeify_case('helloWORLDEarth'),
        "camel_hyphens" : snowplow_normalize.snakeify_case('helloWorld-Earth'),
        "camel_numbers" : snowplow_normalize.snakeify_case('helloWorld23Earth')
        }
    %}


    {{ dbt_unittest.assert_equals(expected_dict, results_dict) }}


{% endmacro %}
```
</TabItem>
</Tabs>

</DbtDetails>

</DbtDetails>

### Test Users Table {#macro.snowplow_normalize_integration_tests.test_users_table}

<DbtDetails><summary>
<code>macros/test_users_table.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code</summary>

<Tabs groupId="dispatched_sql">
<TabItem value="raw" label="Raw" default>

```jinja2
{% macro test_users_table() %}

    {{ return(adapter.dispatch('test_users_table', 'snowplow_normalize_integration_tests')()) }}

{% endmacro %}
```
</TabItem>
<TabItem value="snowflake" label="snowflake">

```jinja2
{% macro snowflake__test_users_table() %}

    {% set expected_dict = {
            "1_context" : "with defined_user_id as ( select user_id as user_id , collector_tstamp as latest_collector_tstamp -- user column(s) from the event table , CONTEXTS_TEST_1[0]:contextTestId::string as context_test_id , CONTEXTS_TEST_1[0]:contextTestClass::integer as context_test_class from "~target.database~"."~target.schema~"_scratch.snowplow_normalize_base_events_this_run where 1 = 1 ) select * from defined_user_id where user_id is not null qualify row_number() over (partition by user_id order by latest_collector_tstamp desc) = 1",
            "2_context" : "with defined_user_id as ( select user_id as user_id , collector_tstamp as latest_collector_tstamp -- user column(s) from the event table , CONTEXTS_TEST_1[0]:contextTestId::boolean as context_test_id , CONTEXTS_TEST_1[0]:contextTestClass::string as context_test_class , CONTEXT_TEST2_1[0]:contextTestId2::interger as context_test_id2 , CONTEXT_TEST2_1[0]:contextTestClass2::string as context_test_class2 from "~target.database~"."~target.schema~"_scratch.snowplow_normalize_base_events_this_run where 1 = 1 ) select * from defined_user_id where user_id is not null qualify row_number() over (partition by user_id order by latest_collector_tstamp desc) = 1",
            "custom_user_field" : "with defined_user_id as ( select test_id as user_id , collector_tstamp as latest_collector_tstamp -- user column(s) from the event table , CONTEXTS_TEST_1[0]:contextTestId::boolean as context_test_id , CONTEXTS_TEST_1[0]:contextTestClass::string as context_test_class , CONTEXT_TEST2_1[0]:contextTestId2::interger as context_test_id2 , CONTEXT_TEST2_1[0]:contextTestClass2::string as context_test_class2 from "~target.database~"."~target.schema~"_scratch.snowplow_normalize_base_events_this_run where 1 = 1 ) select * from defined_user_id where user_id is not null qualify row_number() over (partition by user_id order by latest_collector_tstamp desc) = 1",
            "custom_user_field_sde" : "with defined_user_id as ( select UNSTRUCT_EVENT_COM_GOOGLE_ANALYTICS_MEASUREMENT_PROTOCOL_USER_1:testId::string as user_id , collector_tstamp as latest_collector_tstamp -- user column(s) from the event table , CONTEXTS_TEST_1[0]:contextTestId::boolean as context_test_id , CONTEXTS_TEST_1[0]:contextTestClass::string as context_test_class , CONTEXT_TEST2_1[0]:contextTestId2::interger as context_test_id2 , CONTEXT_TEST2_1[0]:contextTestClass2::string as context_test_class2 from "~target.database~"."~target.schema~"_scratch.snowplow_normalize_base_events_this_run where 1 = 1 ) select * from defined_user_id where user_id is not null qualify row_number() over (partition by user_id order by latest_collector_tstamp desc) = 1",
            "custom_user_field_context" : "with defined_user_id as ( select CONTEXTS_COM_ZENDESK_SNOWPLOW_USER_1[0]:testId::string as user_id , collector_tstamp as latest_collector_tstamp -- user column(s) from the event table , CONTEXTS_TEST_1[0]:contextTestId::boolean as context_test_id , CONTEXTS_TEST_1[0]:contextTestClass::string as context_test_class , CONTEXT_TEST2_1[0]:contextTestId2::interger as context_test_id2 , CONTEXT_TEST2_1[0]:contextTestClass2::string as context_test_class2 from "~target.database~"."~target.schema~"_scratch.snowplow_normalize_base_events_this_run where 1 = 1 ) select * from defined_user_id where user_id is not null qualify row_number() over (partition by user_id order by latest_collector_tstamp desc) = 1",
            "custom_user_field_both" : "with defined_user_id as ( select UNSTRUCT_EVENT_COM_GOOGLE_ANALYTICS_MEASUREMENT_PROTOCOL_USER_1:testId::string as user_id , collector_tstamp as latest_collector_tstamp -- user column(s) from the event table , CONTEXTS_TEST_1[0]:contextTestId::boolean as context_test_id , CONTEXTS_TEST_1[0]:contextTestClass::string as context_test_class , CONTEXT_TEST2_1[0]:contextTestId2::interger as context_test_id2 , CONTEXT_TEST2_1[0]:contextTestClass2::string as context_test_class2 from "~target.database~"."~target.schema~"_scratch.snowplow_normalize_base_events_this_run where 1 = 1 ) select * from defined_user_id where user_id is not null qualify row_number() over (partition by user_id order by latest_collector_tstamp desc) = 1"
    } %}



    {% set results_dict ={
        "1_context" : snowplow_normalize.users_table('user_id', '', '', ['CONTEXTS_TEST_1_0_0'], [['contextTestId', 'contextTestClass']], [['string', 'integer']], true).split()|join(' '),
        "2_context" : snowplow_normalize.users_table('user_id', '', '',['CONTEXTS_TEST_1_0_0', 'CONTEXT_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'], ['contextTestId2', 'contextTestClass2']], [['boolean', 'string'], ['interger', 'string']], true).split()|join(' '),
        "custom_user_field" : snowplow_normalize.users_table('testId', '', '',['CONTEXTS_TEST_1_0_0', 'CONTEXT_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'], ['contextTestId2', 'contextTestClass2']], [['boolean', 'string'], ['interger', 'string']], true).split()|join(' '),
        "custom_user_field_sde" : snowplow_normalize.users_table('testId', 'UNSTRUCT_EVENT_COM_GOOGLE_ANALYTICS_MEASUREMENT_PROTOCOL_USER_1_0_0', '',['CONTEXTS_TEST_1_0_0', 'CONTEXT_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'], ['contextTestId2', 'contextTestClass2']], [['boolean', 'string'], ['interger', 'string']], true).split()|join(' '),
        "custom_user_field_context" : snowplow_normalize.users_table('testId', '', 'CONTEXTS_COM_ZENDESK_SNOWPLOW_USER_1_0_0',['CONTEXTS_TEST_1_0_0', 'CONTEXT_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'], ['contextTestId2', 'contextTestClass2']], [['boolean', 'string'], ['interger', 'string']], true).split()|join(' '),
        "custom_user_field_both" : snowplow_normalize.users_table('testId', 'UNSTRUCT_EVENT_COM_GOOGLE_ANALYTICS_MEASUREMENT_PROTOCOL_USER_1_0_0', 'CONTEXTS_COM_ZENDESK_SNOWPLOW_USER_1_0_0',['CONTEXTS_TEST_1_0_0', 'CONTEXT_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'], ['contextTestId2', 'contextTestClass2']], [['boolean', 'string'], ['interger', 'string']], true).split()|join(' '),
        }
    %}


    {# {{ print(results_dict['1_context'])}} #}
    {# {{ print(results_dict['2_context'])}} #}
    {# {{ print(results_dict['custom_user_field'])}} #}
    {# {{ print(results_dict['custom_user_field_sde'])}} #}
    {# {{ print(results_dict['custom_user_field_context'])}} #}
    {# {{ print(results_dict['custom_user_field_both'])}} #}


    {{ dbt_unittest.assert_equals(expected_dict, results_dict) }}


{% endmacro %}
```
</TabItem>
<TabItem value="bigquery" label="bigquery">

```jinja2
{% macro bigquery__test_users_table() %}

    {% set expected_dict = {
        "1_context" : "with defined_user_id as ( select user_id as user_id , collector_tstamp as latest_collector_tstamp -- user column(s) from the event table , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_id) as context_test_id , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_class) as context_test_class from `"~target.project~"`."~target.dataset~"_scratch.snowplow_normalize_base_events_this_run where 1 = 1 ), users_ordering as ( select a.* , row_number() over (partition by user_id order by latest_collector_tstamp desc) as rn from defined_user_id a where user_id is not null ) select * except (rn) from users_ordering where rn = 1",
        "2_context" : "with defined_user_id as ( select user_id as user_id , collector_tstamp as latest_collector_tstamp -- user column(s) from the event table , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_id) as context_test_id , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_class) as context_test_class , coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_id2, contexts_test2_1_0_4[safe_offset(0)].context_test_id2, contexts_test2_1_0_3[safe_offset(0)].context_test_id2, contexts_test2_1_0_2[safe_offset(0)].context_test_id2, contexts_test2_1_0_1[safe_offset(0)].context_test_id2, contexts_test2_1_0_0[safe_offset(0)].context_test_id2) as context_test_id2 , coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_class2, contexts_test2_1_0_4[safe_offset(0)].context_test_class2, contexts_test2_1_0_3[safe_offset(0)].context_test_class2, contexts_test2_1_0_2[safe_offset(0)].context_test_class2, contexts_test2_1_0_1[safe_offset(0)].context_test_class2, contexts_test2_1_0_0[safe_offset(0)].context_test_class2) as context_test_class2 from `"~target.project~"`."~target.dataset~"_scratch.snowplow_normalize_base_events_this_run where 1 = 1 ), users_ordering as ( select a.* , row_number() over (partition by user_id order by latest_collector_tstamp desc) as rn from defined_user_id a where user_id is not null ) select * except (rn) from users_ordering where rn = 1",
        "custom_user_field" : "with defined_user_id as ( select test_id as user_id , collector_tstamp as latest_collector_tstamp -- user column(s) from the event table , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_id) as context_test_id , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_class) as context_test_class , coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_id2, contexts_test2_1_0_4[safe_offset(0)].context_test_id2, contexts_test2_1_0_3[safe_offset(0)].context_test_id2, contexts_test2_1_0_2[safe_offset(0)].context_test_id2, contexts_test2_1_0_1[safe_offset(0)].context_test_id2, contexts_test2_1_0_0[safe_offset(0)].context_test_id2) as context_test_id2 , coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_class2, contexts_test2_1_0_4[safe_offset(0)].context_test_class2, contexts_test2_1_0_3[safe_offset(0)].context_test_class2, contexts_test2_1_0_2[safe_offset(0)].context_test_class2, contexts_test2_1_0_1[safe_offset(0)].context_test_class2, contexts_test2_1_0_0[safe_offset(0)].context_test_class2) as context_test_class2 from `"~target.project~"`."~target.dataset~"_scratch.snowplow_normalize_base_events_this_run where 1 = 1 ), users_ordering as ( select a.* , row_number() over (partition by user_id order by latest_collector_tstamp desc) as rn from defined_user_id a where user_id is not null ) select * except (rn) from users_ordering where rn = 1",
        "custom_user_field_sde" : "with defined_user_id as ( select coalesce(unstruct_event_test_1_0_1.test_id) as user_id , collector_tstamp as latest_collector_tstamp -- user column(s) from the event table , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_id) as context_test_id , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_class) as context_test_class , coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_id2, contexts_test2_1_0_4[safe_offset(0)].context_test_id2, contexts_test2_1_0_3[safe_offset(0)].context_test_id2, contexts_test2_1_0_2[safe_offset(0)].context_test_id2, contexts_test2_1_0_1[safe_offset(0)].context_test_id2, contexts_test2_1_0_0[safe_offset(0)].context_test_id2) as context_test_id2 , coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_class2, contexts_test2_1_0_4[safe_offset(0)].context_test_class2, contexts_test2_1_0_3[safe_offset(0)].context_test_class2, contexts_test2_1_0_2[safe_offset(0)].context_test_class2, contexts_test2_1_0_1[safe_offset(0)].context_test_class2, contexts_test2_1_0_0[safe_offset(0)].context_test_class2) as context_test_class2 from `"~target.project~"`."~target.dataset~"_scratch.snowplow_normalize_base_events_this_run where 1 = 1 ), users_ordering as ( select a.* , row_number() over (partition by user_id order by latest_collector_tstamp desc) as rn from defined_user_id a where user_id is not null ) select * except (rn) from users_ordering where rn = 1",
        "custom_user_field_context" : "with defined_user_id as ( select coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_id2) as user_id , collector_tstamp as latest_collector_tstamp -- user column(s) from the event table , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_id) as context_test_id , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_class) as context_test_class , coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_id2, contexts_test2_1_0_4[safe_offset(0)].context_test_id2, contexts_test2_1_0_3[safe_offset(0)].context_test_id2, contexts_test2_1_0_2[safe_offset(0)].context_test_id2, contexts_test2_1_0_1[safe_offset(0)].context_test_id2, contexts_test2_1_0_0[safe_offset(0)].context_test_id2) as context_test_id2 , coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_class2, contexts_test2_1_0_4[safe_offset(0)].context_test_class2, contexts_test2_1_0_3[safe_offset(0)].context_test_class2, contexts_test2_1_0_2[safe_offset(0)].context_test_class2, contexts_test2_1_0_1[safe_offset(0)].context_test_class2, contexts_test2_1_0_0[safe_offset(0)].context_test_class2) as context_test_class2 from `"~target.project~"`."~target.dataset~"_scratch.snowplow_normalize_base_events_this_run where 1 = 1 ), users_ordering as ( select a.* , row_number() over (partition by user_id order by latest_collector_tstamp desc) as rn from defined_user_id a where user_id is not null ) select * except (rn) from users_ordering where rn = 1",
        "custom_user_field_both" : "with defined_user_id as ( select coalesce(unstruct_event_test_1_0_1.test_id) as user_id , collector_tstamp as latest_collector_tstamp -- user column(s) from the event table , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_id) as context_test_id , coalesce(contexts_test_1_0_0[safe_offset(0)].context_test_class) as context_test_class , coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_id2, contexts_test2_1_0_4[safe_offset(0)].context_test_id2, contexts_test2_1_0_3[safe_offset(0)].context_test_id2, contexts_test2_1_0_2[safe_offset(0)].context_test_id2, contexts_test2_1_0_1[safe_offset(0)].context_test_id2, contexts_test2_1_0_0[safe_offset(0)].context_test_id2) as context_test_id2 , coalesce(contexts_test2_1_0_5[safe_offset(0)].context_test_class2, contexts_test2_1_0_4[safe_offset(0)].context_test_class2, contexts_test2_1_0_3[safe_offset(0)].context_test_class2, contexts_test2_1_0_2[safe_offset(0)].context_test_class2, contexts_test2_1_0_1[safe_offset(0)].context_test_class2, contexts_test2_1_0_0[safe_offset(0)].context_test_class2) as context_test_class2 from `"~target.project~"`."~target.dataset~"_scratch.snowplow_normalize_base_events_this_run where 1 = 1 ), users_ordering as ( select a.* , row_number() over (partition by user_id order by latest_collector_tstamp desc) as rn from defined_user_id a where user_id is not null ) select * except (rn) from users_ordering where rn = 1"
    } %}



    {% set results_dict ={
        "1_context" : snowplow_normalize.users_table('user_id', '', '', ['CONTEXTS_TEST_1_0_0'], [['contextTestId', 'contextTestClass']], [['string', 'integer']], true).split()|join(' '),
        "2_context" : snowplow_normalize.users_table('user_id', '', '',['CONTEXTS_TEST_1_0_0', 'CONTEXTS_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'], ['contextTestId2', 'contextTestClass2']], [['boolean', 'string'], ['interger', 'string']], true).split()|join(' '),
        "custom_user_field" : snowplow_normalize.users_table('testId', '', '',['CONTEXTS_TEST_1_0_0', 'CONTEXTS_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'], ['contextTestId2', 'contextTestClass2']], [['boolean', 'string'], ['interger', 'string']], true).split()|join(' '),
        "custom_user_field_sde" : snowplow_normalize.users_table('testId', 'UNSTRUCT_EVENT_TEST_1_0_1', '',['CONTEXTS_TEST_1_0_0', 'CONTEXTS_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'], ['contextTestId2', 'contextTestClass2']], [['boolean', 'string'], ['interger', 'string']], true).split()|join(' '),
        "custom_user_field_context" : snowplow_normalize.users_table('contextTestId2', '', 'CONTEXTS_TEST2_1_0_5',['CONTEXTS_TEST_1_0_0', 'CONTEXTS_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'], ['contextTestId2', 'contextTestClass2']], [['boolean', 'string'], ['interger', 'string']], true).split()|join(' '),
        "custom_user_field_both" : snowplow_normalize.users_table('testId', 'UNSTRUCT_EVENT_TEST_1_0_1', 'CONTEXTS_TEST2_1_0_5',['CONTEXTS_TEST_1_0_0', 'CONTEXTS_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'], ['contextTestId2', 'contextTestClass2']], [['boolean', 'string'], ['interger', 'string']], true).split()|join(' '),
        }
    %}


    {# {{ print(results_dict['1_context'])}} #}
    {# {{ print(results_dict['2_context'])}} #}
    {# {{ print(results_dict['custom_user_field'])}} #}
    {# {{ print(results_dict['custom_user_field_sde'])}} #}
    {# {{ print(results_dict['custom_user_field_context'])}} #}
    {# {{ print(results_dict['custom_user_field_both'])}} #}


    {{ dbt_unittest.assert_equals(expected_dict, results_dict) }}


{% endmacro %}
```
</TabItem>
<TabItem value="databricks" label="databricks">

```jinja2
{% macro databricks__test_users_table() %}

    {% set expected_dict = {
            "1_context" : "with defined_user_id as ( select user_id as user_id , collector_tstamp as latest_collector_tstamp , DATE(collector_tstamp) as latest_collector_tstamp_date -- user column(s) from the event table , CONTEXTS_TEST_1[0].context_test_id as context_test_id , CONTEXTS_TEST_1[0].context_test_class as context_test_class from "~target.schema~"_scratch.snowplow_normalize_base_events_this_run where 1 = 1 ), users_ordering as ( select a.* , row_number() over (partition by user_id order by latest_collector_tstamp desc) as rn from defined_user_id a where user_id is not null ) select * except (rn) from users_ordering where rn = 1",
            "2_context" : "with defined_user_id as ( select user_id as user_id , collector_tstamp as latest_collector_tstamp , DATE(collector_tstamp) as latest_collector_tstamp_date -- user column(s) from the event table , CONTEXTS_TEST_1[0].context_test_id as context_test_id , CONTEXTS_TEST_1[0].context_test_class as context_test_class , CONTEXT_TEST2_1[0].context_test_id2 as context_test_id2 , CONTEXT_TEST2_1[0].context_test_class2 as context_test_class2 from "~target.schema~"_scratch.snowplow_normalize_base_events_this_run where 1 = 1 ), users_ordering as ( select a.* , row_number() over (partition by user_id order by latest_collector_tstamp desc) as rn from defined_user_id a where user_id is not null ) select * except (rn) from users_ordering where rn = 1",
            "custom_user_field" : "with defined_user_id as ( select test_id as user_id , collector_tstamp as latest_collector_tstamp , DATE(collector_tstamp) as latest_collector_tstamp_date -- user column(s) from the event table , CONTEXTS_TEST_1[0].context_test_id as context_test_id , CONTEXTS_TEST_1[0].context_test_class as context_test_class , CONTEXT_TEST2_1[0].context_test_id2 as context_test_id2 , CONTEXT_TEST2_1[0].context_test_class2 as context_test_class2 from "~target.schema~"_scratch.snowplow_normalize_base_events_this_run where 1 = 1 ), users_ordering as ( select a.* , row_number() over (partition by user_id order by latest_collector_tstamp desc) as rn from defined_user_id a where user_id is not null ) select * except (rn) from users_ordering where rn = 1",
            "custom_user_field_sde" : "with defined_user_id as ( select UNSTRUCT_EVENT_COM_GOOGLE_ANALYTICS_MEASUREMENT_PROTOCOL_USER_1.test_id as user_id , collector_tstamp as latest_collector_tstamp , DATE(collector_tstamp) as latest_collector_tstamp_date -- user column(s) from the event table , CONTEXTS_TEST_1[0].context_test_id as context_test_id , CONTEXTS_TEST_1[0].context_test_class as context_test_class , CONTEXT_TEST2_1[0].context_test_id2 as context_test_id2 , CONTEXT_TEST2_1[0].context_test_class2 as context_test_class2 from "~target.schema~"_scratch.snowplow_normalize_base_events_this_run where 1 = 1 ), users_ordering as ( select a.* , row_number() over (partition by user_id order by latest_collector_tstamp desc) as rn from defined_user_id a where user_id is not null ) select * except (rn) from users_ordering where rn = 1",
            "custom_user_field_context" : "with defined_user_id as ( select CONTEXTS_COM_ZENDESK_SNOWPLOW_USER_1[0].test_id as user_id , collector_tstamp as latest_collector_tstamp , DATE(collector_tstamp) as latest_collector_tstamp_date -- user column(s) from the event table , CONTEXTS_TEST_1[0].context_test_id as context_test_id , CONTEXTS_TEST_1[0].context_test_class as context_test_class , CONTEXT_TEST2_1[0].context_test_id2 as context_test_id2 , CONTEXT_TEST2_1[0].context_test_class2 as context_test_class2 from "~target.schema~"_scratch.snowplow_normalize_base_events_this_run where 1 = 1 ), users_ordering as ( select a.* , row_number() over (partition by user_id order by latest_collector_tstamp desc) as rn from defined_user_id a where user_id is not null ) select * except (rn) from users_ordering where rn = 1",
            "custom_user_field_both" : "with defined_user_id as ( select UNSTRUCT_EVENT_COM_GOOGLE_ANALYTICS_MEASUREMENT_PROTOCOL_USER_1.test_id as user_id , collector_tstamp as latest_collector_tstamp , DATE(collector_tstamp) as latest_collector_tstamp_date -- user column(s) from the event table , CONTEXTS_TEST_1[0].context_test_id as context_test_id , CONTEXTS_TEST_1[0].context_test_class as context_test_class , CONTEXT_TEST2_1[0].context_test_id2 as context_test_id2 , CONTEXT_TEST2_1[0].context_test_class2 as context_test_class2 from "~target.schema~"_scratch.snowplow_normalize_base_events_this_run where 1 = 1 ), users_ordering as ( select a.* , row_number() over (partition by user_id order by latest_collector_tstamp desc) as rn from defined_user_id a where user_id is not null ) select * except (rn) from users_ordering where rn = 1"
    } %}



    {% set results_dict ={
        "1_context" : snowplow_normalize.users_table('user_id', '', '', ['CONTEXTS_TEST_1_0_0'], [['contextTestId', 'contextTestClass']], [['string', 'integer']], true).split()|join(' '),
        "2_context" : snowplow_normalize.users_table('user_id', '', '',['CONTEXTS_TEST_1_0_0', 'CONTEXT_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'], ['contextTestId2', 'contextTestClass2']], [['boolean', 'string'], ['interger', 'string']], true).split()|join(' '),
        "custom_user_field" : snowplow_normalize.users_table('testId', '', '',['CONTEXTS_TEST_1_0_0', 'CONTEXT_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'], ['contextTestId2', 'contextTestClass2']], [['boolean', 'string'], ['interger', 'string']], true).split()|join(' '),
        "custom_user_field_sde" : snowplow_normalize.users_table('testId', 'UNSTRUCT_EVENT_COM_GOOGLE_ANALYTICS_MEASUREMENT_PROTOCOL_USER_1_0_0', '',['CONTEXTS_TEST_1_0_0', 'CONTEXT_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'], ['contextTestId2', 'contextTestClass2']], [['boolean', 'string'], ['interger', 'string']], true).split()|join(' '),
        "custom_user_field_context" : snowplow_normalize.users_table('testId', '', 'CONTEXTS_COM_ZENDESK_SNOWPLOW_USER_1_0_0',['CONTEXTS_TEST_1_0_0', 'CONTEXT_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'], ['contextTestId2', 'contextTestClass2']], [['boolean', 'string'], ['interger', 'string']], true).split()|join(' '),
        "custom_user_field_both" : snowplow_normalize.users_table('testId', 'UNSTRUCT_EVENT_COM_GOOGLE_ANALYTICS_MEASUREMENT_PROTOCOL_USER_1_0_0', 'CONTEXTS_COM_ZENDESK_SNOWPLOW_USER_1_0_0',['CONTEXTS_TEST_1_0_0', 'CONTEXT_TEST2_1_0_5'], [['contextTestId', 'contextTestClass'], ['contextTestId2', 'contextTestClass2']], [['boolean', 'string'], ['interger', 'string']], true).split()|join(' '),
        }
    %}


    {# {{ print(results_dict['1_context'])}} #}
    {# {{ print(results_dict['2_context'])}} #}
    {# {{ print(results_dict['custom_user_field'])}} #}
    {# {{ print(results_dict['custom_user_field_sde'])}} #}
    {# {{ print(results_dict['custom_user_field_context'])}} #}
    {# {{ print(results_dict['custom_user_field_both'])}} #}


    {{ dbt_unittest.assert_equals(expected_dict, results_dict) }}


{% endmacro %}
```
</TabItem>
</Tabs>

</DbtDetails>

</DbtDetails>
