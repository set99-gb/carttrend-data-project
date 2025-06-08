-- Cette table sera matérialisée sous forme de TABLE (et non une vue)
{{ config(
    materialized='table'
) }}

-- Requête principale : création d'une table de faits des commandes enrichies,
-- qui joint les données stg_commandes, stg_details_commandes, mrt_produits et mrt_promotions
-- afin de calculer les montants avant et après promotion

SELECT
    -- Identifiants et informations principales de la commande
    c.id_commande,
    c.id_client,
    c.id_entrepot_depart,
    c.date_commande,
    c.statut_commande,
    c.mode_de_paiement,
    c.numero_tracking, 
    c.date_livraison_estimee,

    -- Informations du détail de commande
    d.emballage_special,
    d.id_produit, 
    d.quantite, 

    -- Prix unitaire du produit avant toute promo
    prod.prix AS prix_unitaire_avant_promotion,

    -- Montant total de la commande avant toute promotion
    ROUND(d.quantite * prod.prix, 2) AS montant_commande_avant_promotion,

    -- Transformation de l'identifiant de promotion :
    -- Si non vide, on transforme 'P099' en 'PROM099' pour aligner avec la clé primaire des promotions
    CASE 
        WHEN TRIM(c.id_promotion) IS NOT NULL AND TRIM(c.id_promotion) != '' 
        THEN CONCAT('PROM', SUBSTR(c.id_promotion, 2))
        ELSE NULL
    END AS id_promotion,

    -- Champ booléen pour indiquer si une promotion est appliquée ou non
    CASE 
        WHEN TRIM(c.id_promotion) IS NOT NULL AND TRIM(c.id_promotion) != '' 
        THEN TRUE 
        ELSE FALSE 
    END AS promotion_oui_non,

    -- Informations sur la promotion appliquée le cas échééant sinon NULL
    prom.type_promotion, 
    prom.valeur_promotion,

    -- Calcul du montant après application de la promotion - 3 cas : 
    -- 1/ Remise fixe : soustraction directe
    -- 2/ Pourcentage : application d’un taux de réduction
    -- 3/ Autres cas : aucun changement
    -- => Le tout est encadré par GREATEST(..., 0) pour ramener les montants négatifs à 0 
    CASE
        WHEN prom.type_promotion = 'Remise fixe' THEN 
            GREATEST(ROUND((d.quantite * prod.prix) - prom.valeur_promotion, 2), 0)
        WHEN prom.type_promotion = 'Pourcentage' THEN 
            GREATEST(ROUND((d.quantite * prod.prix) * (1 - prom.valeur_promotion), 2), 0)
        ELSE 
            ROUND((d.quantite * prod.prix), 2)
    END AS montant_commande_apres_promotion

-- Jointure des tables sources
FROM {{ ref('stg_commandes') }} AS c

-- Jointure avec stg_details_commandes, sur l'ID de commande
LEFT JOIN {{ ref('stg_details_commandes') }} AS d 
    ON c.id_commande = d.id_commande

-- Jointure avec mrt_dim_produits, pour récupérer le prix
LEFT JOIN {{ ref('mrt_dim_produits') }} AS prod 
    ON d.id_produit = prod.id_produit

-- Jointure avec mrt_dim_promotions :
-- On transforme ici l'identifiant (ex : 'P012') pour le faire correspondre à la clé promotion (ex : 'PROM012')
LEFT JOIN {{ ref('mrt_dim_promotions') }} AS prom 
    ON CONCAT('PROM', SUBSTR(c.id_promotion, 2)) = prom.id_promotion

