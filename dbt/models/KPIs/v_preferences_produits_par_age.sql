-- ==============================================================================
-- Modèle dbt : v_preferences_produits_par_age
-- Objectif : Identifier les produits préférés selon les tranches d'âge
-- Indicateurs : Quantité totale achetée (volume) et chiffre d'affaires (valeur)
-- Classements : rang_volume, rang_valeur
-- ==============================================================================

WITH base_commande AS (
  SELECT
    f.id_client,
    -- Définition des tranches d'âge
    CASE
      WHEN c.age < 25 THEN 'Moins de 25 ans'
      WHEN c.age BETWEEN 25 AND 40 THEN '25–40 ans'
      WHEN c.age BETWEEN 41 AND 60 THEN '41–60 ans'
      ELSE 'Plus de 60 ans'
    END AS tranche_age,
    f.id_produit,
    p.produit,
    SUM(f.quantite) AS quantite_totale,
    SUM(f.quantite * p.prix) AS chiffre_affaires
  FROM {{ ref('mrt_fct_commandes') }} f
  JOIN {{ ref('mrt_dim_clients') }} c ON f.id_client = c.id_client
  JOIN {{ ref('mrt_dim_produits') }} p ON f.id_produit = p.id_produit
  GROUP BY f.id_client, tranche_age, f.id_produit, p.produit
),

-- Agrégation et classement par tranche d'âge
preference_age AS (
  SELECT
    tranche_age,
    id_produit,
    produit,
    SUM(quantite_totale) AS total_achete,
    SUM(chiffre_affaires) AS total_CA,
    RANK() OVER (
      PARTITION BY tranche_age
      ORDER BY SUM(quantite_totale) DESC
    ) AS rang_volume,
    RANK() OVER (
      PARTITION BY tranche_age
      ORDER BY SUM(chiffre_affaires) DESC
    ) AS rang_valeur
  FROM base_commande
  GROUP BY tranche_age, id_produit, produit
)

-- Résultat final : classement des produits par tranche d'âge
SELECT *
FROM preference_age
WHERE rang_volume <= 5 OR rang_valeur <= 5  -- Affiche le top 5 par volume ou par CA
