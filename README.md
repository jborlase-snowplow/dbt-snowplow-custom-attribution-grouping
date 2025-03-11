**Disclaimer: This is not officially supported by Snowplow**

# Snowplow dbt Attribution Model with Custom Groupings

This repository serves to provide a way to run the [Snowplow dbt Attribution Package]([url](https://docs.snowplow.io/docs/modeling-your-data/modeling-your-data-with-dbt/dbt-models/dbt-attribution-data-model/)) that allows you to modify the variables that the attribution is calculated for. By default the Snowplow model only calculates attribution for the Channel and Campaign fields - using this package you can configure this to any field which is available in your paths source table (by default your Snowplow Unified Views table).

# Installation Instructions

1. Install and run the Snowplow Unified and Attribution package
2. Copy and paste the macros within this repo under the macro root directory and add it to your own projects macro root directory where you have the attribution model installed
3. Add the following variable and model configuration to your dbt_project.yml
   ```
   vars:
     snowplow_attribution:
      snowplow__attribution_groupings: ['channel','campaign']
   
   models:
    your_project_name:
      custom_attributions:
        +schema: "derived"
   ```
4. Add any additional groupings you require to the array
5. Copy the example model within this repository's root model folder to your own root model folder
6. Rename the copied model and change the grouping variable on line 36

