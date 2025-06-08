-- ============================================================================
-- Vue : v_produits_achetes_ensemble.sql (US005)
-- Objectif : identifier les paires de produits souvent achetés ensemble
-- avec score de confiance et seuil médian pour analyse Power BI
-- ============================================================================

-- Étape 1 : Historique des produits achetés par client (distincts)
WITH achats_clients AS (
  SELECT 
    id_client,
    id_produit
  FROM {{ ref('mrt_fct_commandes') }}
  WHERE statut_commande != 'Annulée'
  GROUP BY id_client, id_produit
),

-- Étape 2 : Paires de produits achetés ensemble par les mêmes clients
paires_par_client AS (
  SELECT 
    a.id_produit AS produit_1,
    b.id_produit AS produit_2,
    COUNT(DISTINCT a.id_client) AS nb_clients_ayant_les_deux
  FROM achats_clients a
  JOIN achats_clients b 
    ON a.id_client = b.id_client
    AND a.id_produit < b.id_produit
  GROUP BY produit_1, produit_2
),

-- Étape 3 : Nombre de clients acheteurs par produit
achats_par_produit AS (
  SELECT 
    id_produit,
    COUNT(DISTINCT id_client) AS nb_clients_produit
  FROM achats_clients
  GROUP BY id_produit
),

-- Étape 4 : Médiane globale du nombre de clients par paire
seuils AS (
  SELECT 
    APPROX_QUANTILES(nb_clients_ayant_les_deux, 2)[OFFSET(1)] AS mediane_clients_par_paire
  FROM paires_par_client
),

-- Étape 5 : Détails des produits (libellés, catégories, marques)
produits_details AS (
  SELECT 
    id_produit,
    produit,
    categorie,
    sous_categorie,
    marque
  FROM {{ ref('mrt_dim_produits') }}
)

-- Étape 6 : Vue finale enrichie
SELECT 
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

  ROUND(SAFE_DIVIDE(p.nb_clients_ayant_les_deux, ap1.nb_clients_produit), 4) AS confiance_1_vers_2,
  ROUND(SAFE_DIVIDE(p.nb_clients_ayant_les_deux, ap2.nb_clients_produit), 4) AS confiance_2_vers_1,

  s.mediane_clients_par_paire

FROM paires_par_client p
JOIN achats_par_produit ap1 ON p.produit_1 = ap1.id_produit
JOIN achats_par_produit ap2 ON p.produit_2 = ap2.id_produit
JOIN produits_details d1 ON p.produit_1 = d1.id_produit
JOIN produits_details d2 ON p.produit_2 = d2.id_produit
CROSS JOIN seuils s
WHERE p.nb_clients_ayant_les_deux >= 3
ORDER BY p.nb_clients_ayant_les_deux DESC
