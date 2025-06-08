-- je ne sélectionne que les champs voulus (ex : je ne sélectionne pas les champs à caractère personnel, car ils n'ont pas leur utilité en BI/Data mining/Machine Learning)

WITH source AS (
    SELECT
        id_client,
        CAST (age AS INTEGER) AS age, -- je force l'âge comme INTEGER
        genre,
        CAST (frequence_visites AS INTEGER) AS frequence_visites, -- je force la frequence_visites comme INTEGER
        CAST (date_inscription AS DATE) AS date_inscription, -- je force la date_inscription comme DATE
        favoris
    FROM {{ source('dataset_airflow', 'clients') }}
)

SELECT * FROM source
