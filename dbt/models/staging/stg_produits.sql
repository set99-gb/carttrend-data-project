SELECT 
    id AS id_produit, -- renommage du champ, car 'id' tout seul peut-être trompeur notamment lors d'une jointure entre tables ou vues
    categorie,
    marque,
    produit,
    CAST (prix AS FLOAT64) AS prix, -- je convertis le champ en FLOAT
    sous_categorie,
    variation
FROM {{ source('dataset_airflow', 'produits') }}
