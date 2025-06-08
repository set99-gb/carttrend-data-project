-- Vue : v_effets_promotions.sql (US008)
-- Objectif : mesurer l’effet réel des promotions sur les ventes,
-- en comparant le montant théorique (plein tarif) au montant réellement payé
WITH commandes AS (
  SELECT
    id_produit,
    quantite,
    prix_unitaire_avant_promotion,
    montant_commande_apres_promotion AS montant_paye,
    id_promotion
  FROM {{ ref('mrt_fct_commandes') }}
  WHERE statut_commande != 'Annulée'  -- Exclure les commandes annulées
),

calculs AS (
  SELECT
    id_produit,

    -- Montant théorique sans promo (catalogue)
    SUM(COALESCE(quantite, 0) * COALESCE(prix_unitaire_avant_promotion, 0)) AS montant_sans_promo, -- Si quantité est NULL, on consière 0

    -- Montant réellement encaissé (avec ou sans promo)
    SUM(COALESCE(montant_paye, 0)) AS montant_reel,

    -- Nombre de commandes avec et sans promo
    COUNTIF(id_promotion IS NOT NULL) AS nb_commandes_avec_promo,
    COUNTIF(id_promotion IS NULL) AS nb_commandes_sans_promo
  FROM commandes
  GROUP BY id_produit
),

produits AS (
  SELECT 
    id_produit,
    produit,
    categorie,
    marque
  FROM {{ ref('mrt_dim_produits') }}
)

SELECT 
  c.id_produit,
  p.produit,
  p.categorie,
  p.marque,

  c.nb_commandes_avec_promo,
  c.nb_commandes_sans_promo,
  (c.nb_commandes_avec_promo + c.nb_commandes_sans_promo) AS nb_commandes_totales,

  ROUND(SAFE_DIVIDE(c.nb_commandes_avec_promo, c.nb_commandes_avec_promo + c.nb_commandes_sans_promo) * 100, 2) AS part_commandes_promo_pct,

  c.montant_sans_promo,
  c.montant_reel,

  -- Effet global de la promo sur le CA
  ROUND(SAFE_DIVIDE(c.montant_reel - c.montant_sans_promo, c.montant_sans_promo) * 100, 2) AS effet_promo_pct
FROM calculs c
LEFT JOIN produits p ON c.id_produit = p.id_produit
ORDER BY effet_promo_pct ASC
