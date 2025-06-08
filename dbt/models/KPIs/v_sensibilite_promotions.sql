
-- v_sensibilite_promotions.sql (US009)
-- Objectif : analyser la sensibilité des ventes aux promotions

SELECT
  p.id_produit,
  p.produit,
  p.categorie,

  -- Volume vendu avec promotion
  SUM(CASE WHEN f.id_promotion IS NOT NULL THEN COALESCE(f.quantite, 0) ELSE 0 END) AS quantite_en_promo,

  -- Volume vendu sans promotion
  SUM(CASE WHEN f.id_promotion IS NULL THEN COALESCE(f.quantite, 0) ELSE 0 END) AS quantite_hors_promo,

  -- CA total (réellement payé)
  SUM(COALESCE(f.montant_commande_apres_promotion, 0)) AS ca_total,

  -- CA en promotion
  SUM(CASE WHEN f.id_promotion IS NOT NULL THEN COALESCE(f.montant_commande_apres_promotion, 0) ELSE 0 END) AS ca_promo,

  -- Taux de sensibilité en volume
  SAFE_DIVIDE(
    SUM(CASE WHEN f.id_promotion IS NOT NULL THEN f.quantite ELSE 0 END),
    SUM(f.quantite)
  ) AS taux_sensibilite_volume,

  -- Taux de sensibilité en valeur
  SAFE_DIVIDE(
    SUM(CASE WHEN f.id_promotion IS NOT NULL THEN f.montant_commande_apres_promotion ELSE 0 END),
    SUM(f.montant_commande_apres_promotion)
  ) AS taux_sensibilite_valeur,

  -- Rang des produits les plus sensibles
  RANK() OVER (
    ORDER BY SAFE_DIVIDE(
      SUM(CASE WHEN f.id_promotion IS NOT NULL THEN f.quantite ELSE 0 END),
      SUM(f.quantite)
    ) DESC
  ) AS rang_sensibilite_volume

FROM {{ ref('mrt_fct_commandes') }} f
JOIN {{ ref('mrt_dim_produits') }} p ON f.id_produit = p.id_produit

-- Exclure les commandes annulées
WHERE f.statut_commande != 'Annulée'

GROUP BY p.id_produit, p.produit, p.categorie
HAVING SUM(f.quantite) >= 50
ORDER BY taux_sensibilite_volume DESC
