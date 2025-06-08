SELECT
    c.id_entrepot_depart AS id_entrepot,
    e.localisation,
    FORMAT_DATE('%Y-%m', DATE(c.date_commande)) AS annee_mois, -- format année-mois
    COUNT(c.id_commande) AS nombre_commandes

FROM {{ ref('mrt_fct_commandes') }} AS c
JOIN {{ ref('mrt_dim_entrepots') }} AS e ON c.id_entrepot_depart = e.id_entrepot

GROUP BY c.id_entrepot_depart, e.localisation, FORMAT_DATE('%Y-%m', DATE(c.date_commande))
