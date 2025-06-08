-- ============================================================================
-- Vue : v_produits_achetes_ensemble_par_genre.sql (US005)
-- Objectif : Identifier les paires de produits fréquemment achetés ensemble,
-- segmentées par genre (Femme, Homme, Autre...)
-- ============================================================================

WITH clients_avec_genre AS (
  SELECT 
    id_client,
    genre
  FROM {{ ref('mrt_dim_clients') }}
),

achats_clients AS (
  SELECT 
    f.id_client,
    c.genre,
    f.id_produit
  FROM {{ ref('mrt_fct_commandes') }} f
  JOIN clients_avec_genre c ON f.id_client = c.id_client
  WHERE f.statut_commande != 'Annulée'
  GROUP BY f.id_client, c.genre, f.id_produit
),

paires_par_client AS (
  SELECT 
    a.genre,
    a.id_produit AS produit_1,
    b.id_produit AS produit_2,
    COUNT(DISTINCT a.id_client) AS nb_clients_ayant_les_deux
  FROM achats_clients a
  JOIN achats_clients b 
    ON a.id_client = b.id_client
    AND a.id_produit < b.id_produit
    AND a.genre = b.genre
  GROUP BY a.genre, produit_1, produit_2
),

achats_par_produit AS (
  SELECT 
    genre,
    id_produit,
    COUNT(DISTINCT id_client) AS nb_clients_produit
  FROM achats_clients
  GROUP BY genre, id_produit
),

-- Médianes du nombre de clients ayant acheté chaque paire, par genre
seuils AS (
  SELECT
    genre,
    APPROX_QUANTILES(nb_clients_ayant_les_deux, 2)[OFFSET(1)] AS mediane_clients_par_paire
  FROM paires_par_client
  GROUP BY genre
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

-- Résultat final avec confiance et médiane
SELECT 
  p.genre,
  
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
JOIN achats_par_produit ap1 ON p.genre = ap1.genre AND p.produit_1 = ap1.id_produit
JOIN achats_par_produit ap2 ON p.genre = ap2.genre AND p.produit_2 = ap2.id_produit
JOIN produits_details d1 ON p.produit_1 = d1.id_produit
JOIN produits_details d2 ON p.produit_2 = d2.id_produit
JOIN seuils s ON p.genre = s.genre

WHERE p.nb_clients_ayant_les_deux >= 3
ORDER BY p.genre, p.nb_clients_ayant_les_deux DESC
