-- ========================================================
-- Modèle dbt : v_segmentation_produits (US001)
-- Objectif : Segmenter les produits selon leur performance
-- Critères : Quantité vendue et CA réellement encaissé
-- Ajout : exposition des médianes directement dans la vue
-- ========================================================

-- Étape 1 : Agrégation des ventes valides
WITH ventes AS (
  SELECT
    p.id_produit,
    p.produit,
    SUM(COALESCE(f.quantite, 0)) AS quantite_totale,
    SUM(COALESCE(f.montant_commande_apres_promotion, 0)) AS chiffre_affaires
  FROM {{ ref('mrt_fct_commandes') }} f
  JOIN {{ ref('mrt_dim_produits') }} p ON f.id_produit = p.id_produit
  WHERE f.statut_commande != 'Annulée'
  GROUP BY p.id_produit, p.produit
),

-- Étape 2 : Calcul des médianes globales
seuils AS (
  SELECT
    APPROX_QUANTILES(quantite_totale, 2)[OFFSET(1)] AS mediane_quantite,
    APPROX_QUANTILES(chiffre_affaires, 2)[OFFSET(1)] AS mediane_chiffre_affaires
  FROM ventes
),

-- Étape 3 : Classements
classement AS (
  SELECT
    *,
    RANK() OVER (ORDER BY quantite_totale DESC) AS rang_volume,
    RANK() OVER (ORDER BY chiffre_affaires DESC) AS rang_valeur
  FROM ventes
)

-- Étape 4 : Vue finale avec segment + exposition des médianes
SELECT
  c.*,
  s.mediane_quantite,
  s.mediane_chiffre_affaires,

  CASE
    WHEN c.quantite_totale >= s.mediane_quantite AND c.chiffre_affaires >= s.mediane_chiffre_affaires THEN 'Star'
    WHEN c.quantite_totale >= s.mediane_quantite AND c.chiffre_affaires < s.mediane_chiffre_affaires THEN 'Populaire peu rentable'
    WHEN c.quantite_totale < s.mediane_quantite AND c.chiffre_affaires >= s.mediane_chiffre_affaires THEN 'Premium peu vendu'
    ELSE 'Flop'
  END AS segment_produit

FROM classement c
CROSS JOIN seuils s
