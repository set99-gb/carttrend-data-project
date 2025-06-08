-- Les 3 lignes ci-dessous font que dbt créera une table et non une vue

{{ config(
    materialized='table'
) }}

SELECT * 
FROM {{ ref('stg_satisfaction') }}
