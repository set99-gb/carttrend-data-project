-- Les 3 lignes ci-dessous font que dbt créera une table et non une vue

{{ config(
    materialized='table'
) }}

-- je vais récupérer tous les champs du stg_produits, et j'enrichis au passage la table avec des colonnes calculées

SELECT
    id_produit, 
    produit,

    -- on remplace la marque vide ou nulle par 'Divers'

    CASE 
        WHEN marque IS NULL OR TRIM(marque) = '' THEN 'Divers'
        ELSE marque
    END AS marque,
    categorie,

     -- on remplace la sous_catégorie vide ou nulle par la catégorie (et on écrase la colonne 'sous_categorie')

    CASE 
        WHEN sous_categorie IS NULL OR TRIM(sous_categorie) = '' THEN categorie
        ELSE sous_categorie
    END AS sous_categorie,

    -- on impute variation par 'Variation inconnue' si le champ 'variation' initial est vide ou nul
    
    CASE 
       WHEN variation IS NULL OR TRIM(variation) = '' THEN 'Variation inconnue'
        ELSE variation
    END AS variation,
    prix,

FROM {{ ref('stg_produits') }}
