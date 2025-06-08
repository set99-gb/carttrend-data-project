-- ============================================================================
-- Modèle dbt : v_segmentation_categories (US002)
-- Objectif : Identifier les performances commerciales des catégories de produits
-- Méthode : Croisement volume / chiffre d'affaires par catégorie
-- ============================================================================

-- Étape 1 : Regrouper les ventes valides par catégorie
WITH ventes_cat AS (
  SELECT
    p.categorie,
    SUM(COALESCE(f.quantite, 0)) AS quantite_totale,
    SUM(COALESCE(f.montant_commande_apres_promotion, 0)) AS chiffre_affaires
  FROM {{ ref('mrt_fct_commandes') }} f
  JOIN {{ ref('mrt_dim_clients') }} c ON f.id_client = c.id_client
  JOIN {{ ref('mrt_dim_produits') }} p ON f.id_produit = p.id_produit
  WHERE f.statut_commande != 'Annulée'
  GROUP BY p.categorie
),

-- Étape 2 : Médianes globales par catégorie
seuils AS (
  SELECT
    'global' AS cle,
    APPROX_QUANTILES(quantite_totale, 2)[OFFSET(1)] AS mediane_volume,
    APPROX_QUANTILES(chiffre_affaires, 2)[OFFSET(1)] AS mediane_ca
  FROM ventes_cat
),

-- Étape 3 : Rangs par volume et valeur
classement AS (
  SELECT
    v.*,
    RANK() OVER (ORDER BY v.quantite_totale DESC) AS rang_volume,
    RANK() OVER (ORDER BY v.chiffre_affaires DESC) AS rang_valeur
  FROM ventes_cat v
)

-- Étape 4 : Segmentation finale + exposition des médianes
SELECT
  c.categorie,
  c.quantite_totale,
  c.chiffre_affaires,
  c.rang_volume,
  c.rang_valeur,
  s.mediane_volume,
  s.mediane_ca,
  CASE
    WHEN c.quantite_totale >= s.mediane_volume AND c.chiffre_affaires >= s.mediane_ca THEN 'Star'
    WHEN c.quantite_totale >= s.mediane_volume AND c.chiffre_affaires < s.mediane_ca THEN 'Populaire peu rentable'
    WHEN c.quantite_totale < s.mediane_volume AND c.chiffre_affaires >= s.mediane_ca THEN 'Premium discret'
    ELSE 'Faible'
  END AS segment_categorie
FROM classement c
CROSS JOIN seuils s
