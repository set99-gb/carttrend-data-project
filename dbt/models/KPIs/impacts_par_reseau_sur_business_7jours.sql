-- Agréger les données des posts par jour
WITH posts_agg AS (
    SELECT
        FORMAT_DATE('%Y-%m-%d', SAFE_CAST(date_post AS DATE)) AS date_poste,
        SUM(volume_mentions) AS volume_mentions_total,

        -- Comptage des mentions par canal
        SUM(CASE WHEN LOWER(canal_social) = 'instagram' THEN volume_mentions ELSE 0 END) AS nombre_instagram,
        SUM(CASE WHEN LOWER(canal_social) = 'twitter' THEN volume_mentions ELSE 0 END) AS nombre_twitter,
        SUM(CASE WHEN LOWER(canal_social) = 'tiktok' THEN volume_mentions ELSE 0 END) AS nombre_tiktok,
        SUM(CASE WHEN LOWER(canal_social) = 'facebook' THEN volume_mentions ELSE 0 END) AS nombre_facebook,
        SUM(CASE WHEN LOWER(canal_social) = 'linkedin' THEN volume_mentions ELSE 0 END) AS nombre_linkedin

    FROM {{ ref('mrt_fct_posts') }}
    GROUP BY date_poste
),

-- Comptage des commandes sur 7 jours suivant chaque date de post
commandes_7j_agg AS (
    SELECT
        p.date_poste,
        COUNT(*) AS commande_7_jours
    FROM posts_agg p
    LEFT JOIN {{ ref('mrt_fct_commandes') }} c
        ON DATE(c.date_commande) BETWEEN DATE(p.date_poste) AND DATE_ADD(DATE(p.date_poste), INTERVAL 7 DAY)
    GROUP BY p.date_poste
)

-- Requête principale
SELECT
    p.date_poste,
    p.volume_mentions_total,
    p.nombre_instagram,
    p.nombre_twitter,
    p.nombre_tiktok,
    p.nombre_facebook,
    p.nombre_linkedin,
    COALESCE(c7.commande_7_jours, 0) AS commande_7_jours
FROM posts_agg p
LEFT JOIN commandes_7j_agg c7
    ON p.date_poste = c7.date_poste
ORDER BY p.date_poste
