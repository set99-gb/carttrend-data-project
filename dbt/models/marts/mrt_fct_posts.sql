-- Les 3 lignes ci-dessous font que dbt créera une table et non une vue

{{ config(
    materialized='table'
) }}

SELECT 
    id_post,
    date_post,
    canal_social,
    volume_mentions,
    sentiment_global,
    contenu_post
FROM {{ ref('stg_posts') }}
