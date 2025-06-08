-- Les 3 lignes ci-dessous font que dbt créera une table et non une vue

{{ config(
    materialized='table'
) }}

SELECT 
    id_promotion,
    id_produit,
    type_promotion,

    -- Nettoyage de la colonne valeur_promotion : on traite les % ou les montants (avec € et virgules)
    
    CASE 
        WHEN type_promotion = 'Pourcentage' THEN 
            SAFE_CAST(REPLACE(valeur_promotion, '%', '') AS FLOAT64) / 100
        WHEN type_promotion = 'Remise fixe' THEN 
            SAFE_CAST(
                REGEXP_REPLACE(REPLACE(valeur_promotion, ',', '.'), r'[^\d\.]', '') 
                AS FLOAT64
            )
        ELSE NULL
    END AS valeur_promotion,

    date_debut,
    date_fin,
    responsable_promotion

FROM {{ ref('stg_promotions') }}

