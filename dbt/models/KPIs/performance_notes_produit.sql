--  identifie les produits qui ont été notés plus de 5 fois
WITH produits_notes AS (
    SELECT  
        c.id_produit,
        COUNT(s.note_client) AS nb_notes
    FROM {{ ref('mrt_fct_satisfaction') }} s
    JOIN {{ ref('mrt_fct_commandes') }} c 
        ON s.id_commande = c.id_commande
    WHERE s.note_client IS NOT NULL
    GROUP BY c.id_produit
    HAVING COUNT(s.note_client) > 5  -- On garde uniquement les produits avec plus de 5 notes
)

-- On récupère les détails des commandes et des produits,
-- uniquement pour les produits identifiés ci-dessus
SELECT  
    s.id_commande,
    s.note_client,
    c.date_commande,
    c.date_livraison_estimee,
    FORMAT_DATE('%Y-%m', DATE(c.date_commande)) AS mois_annee,  -- Format année-mois
    c.id_produit,
    p.produit AS nom_produit,
    p.categorie

FROM {{ ref('mrt_fct_satisfaction') }} s
JOIN {{ ref('mrt_fct_commandes') }} c 
    ON s.id_commande = c.id_commande
JOIN {{ ref('mrt_dim_produits') }} p 
    ON c.id_produit = p.id_produit
JOIN produits_notes pn 
    ON c.id_produit = pn.id_produit  -- Filtrage sur les produits avec suffisamment de notes

WHERE s.note_client IS NOT NULL  -- On exclut les lignes sans note client
