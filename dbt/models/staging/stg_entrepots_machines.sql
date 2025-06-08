SELECT
    id AS id_entrepot_machine, 
    id_machine, 
    id_entrepot, 
    type_machine, 
    etat_machine, 
    CAST (temps_darret AS INTEGER) AS temps_arret, -- converstion en INTEGER et renommage du champ pour plus de clarté
    CAST (volume_traite AS INTEGER) AS volume_traite, -- converstion en INTEGER 
    mois AS annee_mois --renommage pour plus de clarté
FROM {{ source('dataset_airflow', 'entrepots_machines') }}