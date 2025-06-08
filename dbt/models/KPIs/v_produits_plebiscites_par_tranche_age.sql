-- ============================================================================
-- Vue : v_produits_plebiscites_par_tranche_age (US003)
-- Objectif : Identifier les produits préférés par tranche d’âge
-- Méthode : Quantité et CA + comparaison aux médianes de la tranche
-- ============================================================================

WITH ventes_age AS (
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
),

-- Médianes par tranche d'âge
seuils AS (
  SELECT
    tranche_age,
    APPROX_QUANTILES(quantite_totale, 2)[OFFSET(1)] AS mediane_volume,
    APPROX_QUANTILES(chiffre_affaires, 2)[OFFSET(1)] AS mediane_ca
  FROM ventes_age
  GROUP BY tranche_age
),

-- Rangs par volume et CA dans chaque tranche
classement AS (
  SELECT
    v.*,
    RANK() OVER (PARTITION BY v.tranche_age ORDER BY v.quantite_totale DESC) AS rang_volume,
    RANK() OVER (PARTITION BY v.tranche_age ORDER BY v.chiffre_affaires DESC) AS rang_valeur
  FROM ventes_age v
)

-- Résultat enrichi avec médianes
SELECT
  c.tranche_age,
  c.id_produit,
  c.produit,
  c.categorie,
  c.quantite_totale,
  c.chiffre_affaires,
  c.rang_volume,
  c.rang_valeur,
  s.mediane_volume,
  s.mediane_ca
FROM classement c
JOIN seuils s ON c.tranche_age = s.tranche_age
WHERE c.rang_volume <= 10
ORDER BY c.tranche_age, c.rang_volume
