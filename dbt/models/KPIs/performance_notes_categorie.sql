-- Moyenne des notes par catégorie et par mois de commande
SELECT 
    p.categorie,
    c.id_produit,
    FORMAT_DATE('%Y-%m', DATE(c.date_commande)) AS mois_commande,
    AVG(s.note_client) AS note_moyenne,
    COUNT(*) AS nb_avis
FROM {{ ref('mrt_fct_satisfaction') }} s
JOIN {{ ref('mrt_fct_commandes') }} c ON s.id_commande = c.id_commande
JOIN {{ ref('mrt_dim_produits') }} p ON c.id_produit = p.id_produit
GROUP BY p.categorie, c.id_produit, mois_commande
ORDER BY mois_commande ASC, note_moyenne DESC


