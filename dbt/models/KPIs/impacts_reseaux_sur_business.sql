-- Agréger les données des posts par mois
WITH posts_agg AS (
    SELECT
        FORMAT_DATE('%Y-%m', SAFE_CAST(date_post AS DATE)) AS annee_mois_post,
        SUM(CASE WHEN sentiment_global = 'Neutre' THEN volume_mentions ELSE 0 END) AS volumes_mentions_neutres,
        SUM(CASE WHEN sentiment_global = 'Positif' THEN volume_mentions ELSE 0 END) AS volumes_mentions_positives,
        SUM(CASE WHEN sentiment_global = 'Négatif' THEN volume_mentions ELSE 0 END) AS volumes_mentions_negatives,
        SUM(volume_mentions) AS volume_mentions_total
    FROM {{ ref('mrt_fct_posts') }}
    GROUP BY annee_mois_post
),

-- Agréger les données des commandes par mois
commandes_agg AS (
    SELECT
        FORMAT_DATE('%Y-%m', SAFE_CAST(date_commande AS DATE)) AS annee_mois_commande,
        COUNTIF(statut_commande != 'Annulée') AS volume_commandes_viables,
        COUNTIF(statut_commande = 'Annulée') AS volume_commandes_annulees,
        COUNT(*) AS volume_commandes_total
    FROM {{ ref('mrt_fct_commandes') }}
    GROUP BY annee_mois_commande
),

-- Agréger les notes moyennes par mois via jointure satisfaction → commandes
notes_agg AS (
    SELECT
        FORMAT_DATE('%Y-%m', SAFE_CAST(c.date_commande AS DATE)) AS annee_mois_note,
        ROUND(AVG(s.note_client),2) AS note_moyenne
    FROM {{ ref('mrt_fct_satisfaction') }} s
    JOIN {{ ref('mrt_fct_commandes') }} c
        ON s.id_commande = c.id_commande
    GROUP BY annee_mois_note
)

-- Requête principale
SELECT
    p.annee_mois_post,
    p.volumes_mentions_neutres,
    p.volumes_mentions_positives,
    p.volumes_mentions_negatives,
    c.volume_commandes_viables,
    c.volume_commandes_annulees,
    c.volume_commandes_total,
    p.volume_mentions_total,
    n.note_moyenne
FROM posts_agg p
LEFT JOIN commandes_agg c
    ON p.annee_mois_post = c.annee_mois_commande
LEFT JOIN notes_agg n
    ON p.annee_mois_post = n.annee_mois_note
ORDER BY p.annee_mois_post
