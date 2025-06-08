-- Agrégation des données machines : somme du temps d'arrêt EN PANNE, volume traité, nombre de mois où la machine a été marquée en panne et annee_mois
WITH machines_ag AS (
  SELECT
    id_machine,
    id_entrepot,
    type_machine,
    annee_mois,
    -- Somme du temps d'arrêt pour les machines en panne
    SUM(CASE WHEN etat_machine = 'En panne' THEN temps_arret ELSE 0 END) AS temps_arret_panne,
    -- Nombre de fois où la machine est en panne
    SUM(CASE WHEN etat_machine = 'En panne' THEN 1 ELSE 0 END) AS nb_pannes_machines,
    -- Volume total traité
    SUM(volume_traite) AS total_volume_traite
  FROM {{ ref('mrt_fct_machines') }}
  GROUP BY id_machine, id_entrepot, type_machine, annee_mois
),

-- Moyenne du délai de livraison par entrepôt et mois
delai_livraison AS (
  SELECT
    id_entrepot_depart AS id_entrepot,
    FORMAT_DATE('%Y-%m', DATE(date_commande)) AS annee_mois,
    AVG(DATE_DIFF(DATE(date_livraison_estimee), DATE(date_commande), DAY)) AS delai_moyen_livraison_entrepot_mois
  FROM {{ ref('mrt_fct_commandes') }}
  GROUP BY id_entrepot, annee_mois
), 

nom_entrepot AS (
    SELECT
        id_entrepot,
        localisation
    FROM {{ ref('mrt_dim_entrepots') }}
)

-- Jointure finale : machines avec délai moyen de livraison par entrepôt et mois
SELECT
  m.id_machine,
  m.id_entrepot,
  ne.localisation,
  m.type_machine,
  m.annee_mois,
  m.temps_arret_panne,
  m.nb_pannes_machines,
  m.total_volume_traite AS volume_traite,
  d.delai_moyen_livraison_entrepot_mois

FROM machines_ag AS m

LEFT JOIN delai_livraison AS d
    ON m.id_entrepot = d.id_entrepot AND m.annee_mois = d.annee_mois

LEFT JOIN nom_entrepot AS ne 
    ON m.id_entrepot = ne.id_entrepot

order by annee_mois