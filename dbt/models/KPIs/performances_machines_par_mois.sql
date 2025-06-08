WITH delais AS (
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

commandes_par_entrepot AS (
  SELECT
    c.id_entrepot_depart,
    COUNT(*) AS nb_commandes,
    SUM(CASE
      WHEN DATE_DIFF(DATE(c.date_livraison_estimee), DATE(c.date_commande), DAY) > m.mediane
      THEN 1 ELSE 0 END
    ) AS nombre_retards
  FROM {{ ref('mrt_fct_commandes') }} c
  CROSS JOIN mediane_delai m
  GROUP BY c.id_entrepot_depart
)

SELECT
    m.id_entrepot,
    e.localisation,
    m.annee_mois,
    m.id_machine,
    m.type_machine,
    m.volume_traite,

FROM {{ ref('mrt_fct_machines') }} AS m

JOIN {{ ref('mrt_dim_entrepots') }} AS e
ON m.id_entrepot = e.id_entrepot

LEFT JOIN commandes_par_entrepot AS cpe
ON m.id_entrepot = cpe.id_entrepot_depart
