SELECT
    id_post,
    CAST (date_post AS DATE) AS date_post, -- je change le type en DATE 
    canal_social, 
    CAST (volume_mentions AS INTEGER) AS volume_mentions, -- je change le type en INTEGER 
    sentiment_global, 
    contenu_post
FROM {{ source('dataset_airflow', 'posts') }}
