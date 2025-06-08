SELECT
    id_commande,
    id_produit, 
    CAST (quantite AS INTEGER) AS quantite, -- je change le typee du champ en INTEGER 

    -- conversion du champ emballage_special en booléen 

    CASE 
        WHEN emballage_special = 'Oui' THEN TRUE
        WHEN emballage_special = 'Non' THEN FALSE 
        ELSE NULL 
    END AS emballage_special,

FROM {{ source('dataset_airflow', 'details_commandes') }}
