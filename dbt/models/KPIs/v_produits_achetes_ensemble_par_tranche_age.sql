-- ============================================================================
-- Vue : v_produits_achetes_ensemble_par_tranche_age.sql (US005)
-- Objectif : identifier les paires de produits achetés ensemble selon l'âge
-- Méthode : regroupement client, paires croisées, score de confiance + médianes
-- ============================================================================

WITH clients_avec_tranche AS (
  SELECT 
    id_client,
    CASE 
      WHEN age < 25 THEN 'moins_25'
      WHEN age BETWEEN 25 AND 40 THEN 'entre_25_40'
      WHEN age BETWEEN 41 AND 60 THEN 'entre_41_60'
      ELSE 'plus_60'
    END AS tranche_age
  FROM {{ ref('mrt_dim_clients') }}
),

achats_clients AS (
  SELECT 
    f.id_client,
    c.tranche_age,
    f.id_produit
  FROM {{ ref('mrt_fct_commandes') }} f
  JOIN clients_avec_tranche c ON f.id_client = c.id_client
  WHERE f.statut_commande != 'Annulée'
  GROUP BY f.id_client, c.tranche_age, f.id_produit
),

paires_par_client AS (
  SELECT 
    a.tranche_age,
    a.id_produit AS produit_1,
    b.id_produit AS produit_2,
    COUNT(DISTINCT a.id_client) AS nb_clients_ayant_les_deux
  FROM achats_clients a
  JOIN achats_clients b 
    ON a.id_client = b.id_client
    AND a.id_produit < b.id_produit
    AND a.tranche_age = b.tranche_age
  GROUP BY a.tranche_age, produit_1, produit_2
),

achats_par_produit AS (
  SELECT 
    tranche_age,
    id_produit,
    COUNT(DISTINCT id_client) AS nb_clients_produit
  FROM achats_clients
  GROUP BY tranche_age, id_produit
),

-- Médianes par tranche d'âge
seuils AS (
  SELECT
    tranche_age,
    APPROX_QUANTILES(nb_clients_ayant_les_deux, 2)[OFFSET(1)] AS mediane_clients_par_paire
  FROM paires_par_client
  GROUP BY tranche_age
),

produits_details AS (
  SELECT 
    id_produit,
    produit,
    categorie,
    sous_categorie,
    marque
  FROM {{ ref('mrt_dim_produits') }}
)

-- Résultat final enrichi
SELECT 
  p.tranche_age,

  p.produit_1,
  d1.produit AS produit_libelle_1,
  d1.categorie AS categorie_1,
  d1.marque AS marque_1,

  p.produit_2,
  d2.produit AS produit_libelle_2,
  d2.categorie AS categorie_2,
  d2.marque AS marque_2,

  p.nb_clients_ayant_les_deux,
  ap1.nb_clients_produit AS nb_clients_produit_1,
  ap2.nb_clients_produit AS nb_clients_produit_2,

  SAFE_DIVIDE(p.nb_clients_ayant_les_deux, ap1.nb_clients_produit) AS confiance_1_vers_2,
  SAFE_DIVIDE(p.nb_clients_ayant_les_deux, ap2.nb_clients_produit) AS confiance_2_vers_1,

  s.mediane_clients_par_paire

FROM paires_par_client p
JOIN achats_par_produit ap1 ON p.tranche_age = ap1.tranche_age AND p.produit_1 = ap1.id_produit
JOIN achats_par_produit ap2 ON p.tranche_age = ap2.tranche_age AND p.produit_2 = ap2.id_produit
JOIN produits_details d1 ON p.produit_1 = d1.id_produit
JOIN produits_details d2 ON p.produit_2 = d2.id_produit
JOIN seuils s ON p.tranche_age = s.tranche_age

WHERE p.nb_clients_ayant_les_deux >= 3
ORDER BY p.tranche_age, p.nb_clients_ayant_les_deux DESC
