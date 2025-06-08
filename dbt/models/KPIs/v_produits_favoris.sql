-- ========================================================
-- Modèle dbt : v_produits_favoris (US004)
-- Objectif : Favoris produits — Identifier les produits les plus souvent ajoutés aux favoris, indépendamment des ventes.
-- Critères : Quantité vendue et CA réellement encaissé
-- Ajout : exposition des médianes directement dans la vue
-- ========================================================


SELECT
  f.id_produit_favori,
  p.produit AS nom_produit_favoris,
  p.categorie,
  p.sous_categorie,
  COUNT(DISTINCT f.id_client) AS nb_clients_ayant_favori
FROM {{ ref('mrt_rel_produits_favoris') }} f
JOIN {{ ref('mrt_dim_produits') }} p
  ON f.id_produit_favori = p.id_produit
GROUP BY f.id_produit_favori, p.produit, p.categorie, p.sous_categorie
ORDER BY nb_clients_ayant_favori DESC