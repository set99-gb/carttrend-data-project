SELECT
    id_promotion,
    id_produit,
    type_promotion,
    valeur_promotion,

    -- je change le type des 2 champs ci dessous en DATE 

    CAST (date_debut AS DATE) AS date_debut,
    CAST (date_fin AS DATE) AS date_fin, 
    responsable_promotion
    
FROM {{ source('dataset_airflow', 'promotions') }}
