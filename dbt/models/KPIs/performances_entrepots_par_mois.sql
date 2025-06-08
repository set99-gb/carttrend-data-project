WITH volumes_par_entrepot AS (
  SELECT
    m.id_entrepot,
    m.annee_mois,
    SUM(m.volume_traite) AS total_volumes
  FROM {{ ref('mrt_fct_machines') }} m
  GROUP BY m.id_entrepot, m.annee_mois
),

delais AS (
  SELECT
    DATE_DIFF(DATE(c.date_livraison_estimee), DATE(c.date_commande), DAY) AS delai
  FROM {{ ref('mrt_fct_commandes') }} c
),

mediane_delai AS (
  SELECT
    PERCENTILE_CONT(delai, 0.5) OVER () AS mediane
  FROM delais
  LIMIT 1
),

commandes_par_entrepot_par_mois AS (
  SELECT
    c.id_entrepot_depart,
    FORMAT_DATE('%Y-%m', DATE(c.date_commande)) AS annee_mois,
    COUNT(*) AS nb_commandes,
    SUM(CASE
      WHEN DATE_DIFF(DATE(c.date_livraison_estimee), DATE(c.date_commande), DAY) > m.mediane
      THEN 1 ELSE 0 END
    ) AS nb_retards
  FROM {{ ref('mrt_fct_commandes') }} c
  CROSS JOIN mediane_delai m
  GROUP BY c.id_entrepot_depart, FORMAT_DATE('%Y-%m', DATE(c.date_commande))
)

SELECT
  e.id_entrepot,
  e.localisation,
  COALESCE(v.annee_mois, c.annee_mois) AS annee_mois,
  COALESCE(v.total_volumes, 0) AS total_volumes,
  COALESCE(c.nb_retards, 0) AS nb_retards
FROM {{ ref('mrt_dim_entrepots') }} e
LEFT JOIN volumes_par_entrepot v ON e.id_entrepot = v.id_entrepot
LEFT JOIN commandes_par_entrepot_par_mois c ON e.id_entrepot = c.id_entrepot_depart AND v.annee_mois = c.annee_mois
ORDER BY id_entrepot, annee_mois ASC
