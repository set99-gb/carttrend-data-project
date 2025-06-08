-- Agréger les données des posts par jour
WITH posts_agg AS (
    SELECT
        FORMAT_DATE('%Y-%m-%d', SAFE_CAST(date_post AS DATE)) AS date_poste,
        SUM(CASE WHEN sentiment_global = 'Neutre' THEN volume_mentions ELSE 0 END) AS volumes_mentions_neutres,
        SUM(CASE WHEN sentiment_global = 'Positif' THEN volume_mentions ELSE 0 END) AS volumes_mentions_positives,
        SUM(CASE WHEN sentiment_global = 'Négatif' THEN volume_mentions ELSE 0 END) AS volumes_mentions_negatives,
        SUM(volume_mentions) AS volume_mentions_total
    FROM {{ ref('mrt_fct_posts') }}
    GROUP BY date_poste
),

-- Comptage des commandes sur 7 jours suivant chaque date de post
commandes_7j_agg AS (
    SELECT
        p.date_poste,
        COUNT(*) AS commande_7_jours
    FROM posts_agg p
    JOIN {{ ref('mrt_fct_commandes') }} c
        ON DATE(c.date_commande) BETWEEN DATE(p.date_poste) AND DATE_ADD(DATE(p.date_poste), INTERVAL 7 DAY)
    GROUP BY p.date_poste
)

-- Requête principale
SELECT
    p.date_poste,
    p.volumes_mentions_neutres,
    p.volumes_mentions_positives,
    p.volumes_mentions_negatives,
    p.volume_mentions_total,
    COALESCE(c7.commande_7_jours, 0) AS commande_7_jours
FROM posts_agg p
LEFT JOIN commandes_7j_agg c7
    ON p.date_poste = c7.date_poste
ORDER BY p.date_poste
