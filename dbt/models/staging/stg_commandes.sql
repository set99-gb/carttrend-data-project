SELECT
    id_commande,
    id_client,
    id_entrepot_depart,
    CAST (date_commande AS DATE) AS date_commande, --je change le type en DATE
    statut_commande, 
    id_promotion_appliquee AS id_promotion, -- je renomme le champ pour plus de clarté et concordance avec le même champ de stg_promotions
    mode_de_paiement,
    numero_tracking,
    CAST (date_livraison_estimee AS DATE) AS date_livraison_estimee --je change le type en DATE

FROM {{ source('dataset_airflow', 'commandes') }}
