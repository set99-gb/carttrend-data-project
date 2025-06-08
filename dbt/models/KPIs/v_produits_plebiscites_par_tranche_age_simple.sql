-- ============================================================================
-- Vue : v_produits_plebiscites_par_tranche_age_simple (US003a)
-- Objectif : Produits les plus achetés par tranche d’âge (volume + CA)
-- ============================================================================

SELECT
  CASE
    WHEN c.age < 25 THEN 'Moins de 25 ans'
    WHEN c.age BETWEEN 25 AND 40 THEN '25–40 ans'
    WHEN c.age BETWEEN 41 AND 60 THEN '41–60 ans'
    ELSE 'Plus de 60 ans'
  END AS tranche_age,

  p.id_produit,
  p.produit,
  p.categorie,

  SUM(COALESCE(f.quantite, 0)) AS quantite_totale,
  SUM(COALESCE(f.montant_commande_apres_promotion, 0)) AS chiffre_affaires

FROM {{ ref('mrt_fct_commandes') }} f
JOIN {{ ref('mrt_dim_clients') }} c ON f.id_client = c.id_client
JOIN {{ ref('mrt_dim_produits') }} p ON f.id_produit = p.id_produit
WHERE f.statut_commande != 'Annulée'

GROUP BY tranche_age, p.id_produit, p.produit, p.categorie
