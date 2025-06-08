SELECT
    id_satisfaction,
    id_commande,
    CAST(note_client AS INTEGER) AS note_client,
    commentaire,

    -- je convertis ce champ en BOOLEAN : Yes passe à true ; No passe à false et NULL reste à NULL
    CASE
        WHEN plainte = 'Yes' THEN TRUE
        WHEN plainte = 'No' THEN FALSE
        ELSE NULL
    END AS plainte,

    CAST(temps_reponse_support AS INTEGER) AS temps_reponse_support, -- je change le type en INTEGER 
    type_plainte, 
    employe_support

FROM {{ source('dataset_airflow', 'satisfaction') }}
