-- ============================================================================
-- Modèle dbt : v_segmentation_produits_par_genre -> Analysre comportementale (US003)
-- Objectif : Identifier les préférences produits selon le genre
-- Méthode : Croisement volume / chiffre d'affaires + exposition des médianes
-- Ajout : exposition des médianes directement dans la vue
-- ============================================================================

-- Étape 1 : Regrouper les ventes valides par genre et produit
WITH ventes_genre AS (
  SELECT
    c.genre,
    p.id_produit,
    p.produit,
    SUM(COALESCE(f.quantite, 0)) AS quantite_totale,
    SUM(COALESCE(f.montant_commande_apres_promotion, 0)) AS chiffre_affaires
  FROM {{ ref('mrt_fct_commandes') }} f
  JOIN {{ ref('mrt_dim_clients') }} c ON f.id_client = c.id_client
  JOIN {{ ref('mrt_dim_produits') }} p ON f.id_produit = p.id_produit
  WHERE f.statut_commande != 'Annulée'
  GROUP BY c.genre, p.id_produit, p.produit
),

-- Étape 2 : Calcul des médianes par genre
seuils AS (
  SELECT
    genre,
    APPROX_QUANTILES(quantite_totale, 2)[OFFSET(1)] AS mediane_volume,
    APPROX_QUANTILES(chiffre_affaires, 2)[OFFSET(1)] AS mediane_ca
  FROM ventes_genre
  GROUP BY genre
),

-- Étape 3 : Attribution des rangs dans chaque genre
classement AS (
  SELECT
    v.*,
    RANK() OVER (PARTITION BY v.genre ORDER BY v.quantite_totale DESC) AS rang_volume,
    RANK() OVER (PARTITION BY v.genre ORDER BY v.chiffre_affaires DESC) AS rang_valeur
  FROM ventes_genre v
)

-- Étape 4 : Segmentation finale + exposition des médianes
SELECT
  c.genre,
  c.id_produit,
  c.produit,
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
  END AS segment_produit
FROM classement c
JOIN seuils s ON c.genre = s.genre
