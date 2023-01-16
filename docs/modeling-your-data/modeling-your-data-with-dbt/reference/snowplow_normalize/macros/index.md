---
title: "Snowplow Normalize Macros"
description: Reference forsnowplow_normalize dbt macros developed by Snowplow
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
## Snowplow Normalize
### Allow Refresh {#macro.snowplow_normalize.allow_refresh}

<DbtDetails><summary>
<code>macros/allow_refresh.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code <a href="https://github.com/snowplow/dbt-snowplow-normalize/blob/main/macros/allow_refresh.sql">(source)</a></summary>

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


#### Depends On
- [macro.snowplow_utils.get_value_by_target](/docs/modeling-your-data/modeling-your-data-with-dbt/reference/snowplow_utils/macros/index.md#macro.snowplow_utils.get_value_by_target)


#### Referenced By
<Tabs groupId="reference">
<TabItem value="model" label="Models" default>

- [model.snowplow_normalize.snowplow_normalize_incremental_manifest](/docs/modeling-your-data/modeling-your-data-with-dbt/reference/snowplow_normalize/models/index.md#model.snowplow_normalize.snowplow_normalize_incremental_manifest)

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
<summary>Code <a href="https://github.com/snowplow/dbt-snowplow-normalize/blob/main/macros/normalize_events.sql">(source)</a></summary>

<Tabs groupId="dispatched_sql">
<TabItem value="raw" label="Raw" default>

```jinja2
{% macro normalize_events(event_names, flat_cols = [], sde_cols = [], sde_keys = [], sde_types = [], sde_aliases = [], context_cols = [], context_keys = [], context_types = [], context_aliases = [], remove_new_event_check = false) %}
    {{ return(adapter.dispatch('normalize_events', 'snowplow_normalize')(event_names, flat_cols, sde_cols, sde_keys, sde_types, sde_aliases, context_cols, context_keys, context_types, context_aliases, remove_new_event_check)) }}
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
</Tabs>

</DbtDetails>


#### Depends On
- [macro.snowplow_utils.is_run_with_new_events](/docs/modeling-your-data/modeling-your-data-with-dbt/reference/snowplow_utils/macros/index.md#macro.snowplow_utils.is_run_with_new_events)
- [macro.snowplow_utils.combine_column_versions](/docs/modeling-your-data/modeling-your-data-with-dbt/reference/snowplow_utils/macros/index.md#macro.snowplow_utils.combine_column_versions)
- [macro.snowplow_normalize.snakeify_case](/docs/modeling-your-data/modeling-your-data-with-dbt/reference/snowplow_normalize/macros/index.md#macro.snowplow_normalize.snakeify_case)


#### Referenced By
<Tabs groupId="reference">
<TabItem value="macros" label="Macros">

- macro.snowplow_normalize_integration_tests.bigquery__test_normalize_events
- macro.snowplow_normalize_integration_tests.databricks__test_normalize_events
- macro.snowplow_normalize_integration_tests.snowflake__test_normalize_events

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
<summary>Code <a href="https://github.com/snowplow/dbt-snowplow-normalize/blob/main/macros/snakeify_case.sql">(source)</a></summary>

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


#### Depends On
- [macro.snowplow_normalize.default__snakeify_case](/docs/modeling-your-data/modeling-your-data-with-dbt/reference/snowplow_normalize/macros/index.md#macro.snowplow_normalize.default__snakeify_case)


#### Referenced By
<Tabs groupId="reference">
<TabItem value="macros" label="Macros">

- macro.snowplow_normalize_integration_tests.default__test_snakeify_case
- [macro.snowplow_normalize.snowflake__users_table](/docs/modeling-your-data/modeling-your-data-with-dbt/reference/snowplow_normalize/macros/index.md#macro.snowplow_normalize.snowflake__users_table)
- [macro.snowplow_normalize.bigquery__users_table](/docs/modeling-your-data/modeling-your-data-with-dbt/reference/snowplow_normalize/macros/index.md#macro.snowplow_normalize.bigquery__users_table)
- [macro.snowplow_normalize.databricks__users_table](/docs/modeling-your-data/modeling-your-data-with-dbt/reference/snowplow_normalize/macros/index.md#macro.snowplow_normalize.databricks__users_table)
- [macro.snowplow_normalize.snowflake__normalize_events](/docs/modeling-your-data/modeling-your-data-with-dbt/reference/snowplow_normalize/macros/index.md#macro.snowplow_normalize.snowflake__normalize_events)
- [macro.snowplow_normalize.bigquery__normalize_events](/docs/modeling-your-data/modeling-your-data-with-dbt/reference/snowplow_normalize/macros/index.md#macro.snowplow_normalize.bigquery__normalize_events)
- [macro.snowplow_normalize.databricks__normalize_events](/docs/modeling-your-data/modeling-your-data-with-dbt/reference/snowplow_normalize/macros/index.md#macro.snowplow_normalize.databricks__normalize_events)

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
<summary>Code <a href="https://github.com/snowplow/dbt-snowplow-normalize/blob/main/macros/users_table.sql">(source)</a></summary>

<Tabs groupId="dispatched_sql">
<TabItem value="raw" label="Raw" default>

```jinja2
{% macro users_table(user_id_field = 'user_id', user_id_sde = '', user_id_context = '', user_cols = [], user_keys = [], user_types = [], remove_new_event_check = false) %}
    {{ return(adapter.dispatch('users_table', 'snowplow_normalize')(user_id_field, user_id_sde, user_id_context, user_cols, user_keys, user_types, remove_new_event_check)) }}
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
</Tabs>

</DbtDetails>


#### Depends On
- [macro.snowplow_utils.is_run_with_new_events](/docs/modeling-your-data/modeling-your-data-with-dbt/reference/snowplow_utils/macros/index.md#macro.snowplow_utils.is_run_with_new_events)
- [macro.snowplow_utils.combine_column_versions](/docs/modeling-your-data/modeling-your-data-with-dbt/reference/snowplow_utils/macros/index.md#macro.snowplow_utils.combine_column_versions)
- [macro.snowplow_normalize.snakeify_case](/docs/modeling-your-data/modeling-your-data-with-dbt/reference/snowplow_normalize/macros/index.md#macro.snowplow_normalize.snakeify_case)


#### Referenced By
<Tabs groupId="reference">
<TabItem value="macros" label="Macros">

- macro.snowplow_normalize_integration_tests.bigquery__test_users_table
- macro.snowplow_normalize_integration_tests.databricks__test_users_table
- macro.snowplow_normalize_integration_tests.snowflake__test_users_table

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
<summary>Code <a href="https://github.com/snowplow/dbt-snowplow-normalize/blob/main/macros/test_normalize_events.sql">(source)</a></summary>

<Tabs groupId="dispatched_sql">
<TabItem value="raw" label="Raw" default>

```jinja2
{% macro test_normalize_events() %}

    {{ return(adapter.dispatch('test_normalize_events', 'snowplow_normalize_integration_tests')()) }}

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
</Tabs>

</DbtDetails>


#### Depends On
- [macro.snowplow_normalize.normalize_events](/docs/modeling-your-data/modeling-your-data-with-dbt/reference/snowplow_normalize/macros/index.md#macro.snowplow_normalize.normalize_events)
- macro.dbt_unittest.assert_dict_equals

</DbtDetails>

### Test Snakeify Case {#macro.snowplow_normalize_integration_tests.test_snakeify_case}

<DbtDetails><summary>
<code>macros/test_snakeify_case.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code <a href="https://github.com/snowplow/dbt-snowplow-normalize/blob/main/macros/test_snakeify_case.sql">(source)</a></summary>

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


#### Depends On
- [macro.snowplow_normalize.snakeify_case](/docs/modeling-your-data/modeling-your-data-with-dbt/reference/snowplow_normalize/macros/index.md#macro.snowplow_normalize.snakeify_case)
- macro.dbt_unittest.assert_equals

</DbtDetails>

### Test Users Table {#macro.snowplow_normalize_integration_tests.test_users_table}

<DbtDetails><summary>
<code>macros/test_users_table.sql</code>
</summary>

#### Description
This macro does not currently have a description.

#### Details
<DbtDetails>
<summary>Code <a href="https://github.com/snowplow/dbt-snowplow-normalize/blob/main/macros/test_users_table.sql">(source)</a></summary>

<Tabs groupId="dispatched_sql">
<TabItem value="raw" label="Raw" default>

```jinja2
{% macro test_users_table() %}

    {{ return(adapter.dispatch('test_users_table', 'snowplow_normalize_integration_tests')()) }}

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
</Tabs>

</DbtDetails>


#### Depends On
- [macro.snowplow_normalize.users_table](/docs/modeling-your-data/modeling-your-data-with-dbt/reference/snowplow_normalize/macros/index.md#macro.snowplow_normalize.users_table)
- macro.dbt_unittest.assert_equals

</DbtDetails>

