{#
Copyright (c) 2024-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}


/* Macro to remove complexity from models paths_to_conversion / paths_to_non_conversion. */

{% macro transform_paths(model_type, source_cte) %}
  {{ return(adapter.dispatch('transform_paths', 'snowplow_attribution')(model_type, source_cte)) }}
{% endmacro %}

{% macro default__transform_paths(model_type, source_cte) %}

  {% set allowed_path_transforms = ['exposure_path', 'first_path', 'remove_if_last_and_not_all', 'remove_if_not_all', 'unique_path'] %}

  , path_transforms as (

    select
      customer_id,
      {% if model_type == 'conversions' %}
        cv_id,
        event_id,
        cv_tstamp,
        cv_type,
        cv_path_start_tstamp,
        revenue,
      {% endif %}

      {% for grouping in var('snowplow__attribution_groupings') %}
        {{ trim_long_path(grouping+'_path', var('snowplow__path_lookback_steps')) }} as {{grouping}}_path,
      {% endfor %}

    {% for grouping in var('snowplow__attribution_groupings') %}

      {% if var('snowplow__path_transforms').items() %}
          -- 1. do transformations on channel_transformed_path:
          -- reverse transormation due to nested functions, items to be processed from left to right
          {% for path_transform_name, _ in var('snowplow__path_transforms').items()|reverse %}
            {% if path_transform_name not in allowed_path_transforms %}
              {%- do exceptions.raise_compiler_error("Snowplow Error: the path transform - '"+path_transform_name+"' - is not supported. Please refer to the Snowplow docs on tagging. Please use one of the following: exposure_path, first_path, remove_if_last_and_not_all, remove_if_not_all, unique_path") %}
            {% endif %}
            {{target.schema}}.{{path_transform_name}}(
          {% endfor %}

          {{grouping}}_transformed_path
          -- no reverse needed due to nested nature of function calls
          {% for _, transform_param in var('snowplow__path_transforms').items() %}
            {% if transform_param %}, '{{transform_param}}' {% endif %}
            )
          {% endfor %}

          as {{grouping}}_transformed_path, 

        {% else %}
        {{grouping}}_transformed_path, 
        {% endif %}

     {% endfor %}
    
  from {{ source_cte }}

  )

{% endmacro %}


{% macro spark__transform_paths(model_type, source_cte) %}

  {% set total_transformations = var('snowplow__path_transforms').items()|length %}
  -- set loop_count using namespace to define it as global variable for the loop to work
  {% set loop_count = namespace(value=1) %}

  -- unlike for adapters using UDFS, reverse transormation is not needed as ctes will process items their params in order
  {% for path_transform_name, transform_param in var('snowplow__path_transforms').items() %}

    {%- if loop_count.value == 1 %}
      {% set previous_cte = source_cte %}
    {% else %}
      {% set previous_cte = loop_count.value-1 %}
    {% endif %}

    , transformation_{{ loop_count.value|string }} as (

      select
        customer_id,
        {% if model_type == 'conversions' %}
          cv_id,
          event_id,
          cv_tstamp,
          cv_type,
          cv_path_start_tstamp,
          revenue
        {% endif %}

        {% for grouping in var('snowplow__attribution_groupings') %}

          ,{{grouping}}_path

          {% if path_transform_name == 'unique_path' %}
            ,{{ path_transformation('unique_path', field_alias=grouping) }}
          {% elif path_transform_name == 'frequency_path' %}
            {{ exceptions.raise_compiler_error(
              "Snowplow Error: Frequency path is currently not supported by the model, please remove it from the variable and use this path transformation function in a custom model."
            ) }}

          {% elif path_transform_name == 'first_path' %}
            ,{{ path_transformation('first_path', field_alias=grouping) }}

          {% elif path_transform_name == 'exposure_path' %}
            ,{{ path_transformation('exposure_path', field_alias=grouping) }}

          {% elif path_transform_name == 'remove_if_not_all' %}
            ,{{ path_transformation('remove_if_not_all', transform_param, grouping) }}

          {% elif path_transform_name == 'remove_if_last_and_not_all' %}
            ,{{ path_transformation('remove_if_last_and_not_all', transform_param, grouping) }}
          
          {% else %}
            {%- do exceptions.raise_compiler_error("Snowplow Error: the path transform - '"+path_transform_name+"' - is not supported. Please refer to the Snowplow docs on tagging. Please use one of the following: exposure_path, first_path, frequency_path, remove_if_last_and_not_all, remove_if_not_all, unique_path") %}
          {% endif %} 
           as {{grouping}}_transformed_path

        {% endfor %}

        {%- if loop_count.value == 1 %}
         from {{ source_cte }}
        {% else %}
        -- build cte names dynamically based on loop count / previous_cte for the loop to work regardless of array items
         from transformation_{{ previous_cte|string }}
        {% endif %}
    )

    {% set previous_cte = loop_count.value %}
    {% set loop_count.value = loop_count.value + 1 %}

  {% endfor %}

  , path_transforms as (

    select
      customer_id,
      {% if model_type == 'conversions' %}
        cv_id,
        event_id,
        cv_tstamp,
        cv_type,
        cv_path_start_tstamp,
        revenue
      {% endif %}

      {% for grouping in var('snowplow__attribution_groupings') %}
        ,{{ trim_long_path(grouping+'_path', var('snowplow__path_lookback_steps')) }} as {{grouping}}_path
        ,{{grouping}}_transformed_path
      {% endfor %}

    from
      -- the last cte will always equal to the total transformations unless there is no item there
      {% if total_transformations > 0 %}
        transformation_{{ total_transformations }}
      {% else %}
        {{ source_cte }}
      {% endif %}
  )

{% endmacro %}
