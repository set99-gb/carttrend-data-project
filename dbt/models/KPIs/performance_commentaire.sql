WITH base AS (
    SELECT
        id_commande,
        commentaire,
        note_client,
        CASE 
            WHEN note_client >= 3 THEN 'positif'
            WHEN note_client <= 2 THEN 'negatif'
            ELSE 'neutre'
        END AS sentiment
    FROM {{ ref('mrt_fct_satisfaction') }}
    WHERE commentaire IS NOT NULL
),
replaced AS (
    SELECT
        id_commande,
        commentaire,
        note_client,
        sentiment,
        CASE
            WHEN LOWER(commentaire) LIKE '%better%' THEN 'passable'
            WHEN LOWER(commentaire) LIKE '%excellent product%' THEN 'excellent'
            WHEN LOWER(commentaire) LIKE '%fast delivery%' THEN 'efficient'
            WHEN LOWER(commentaire) LIKE '%good product%' THEN 'satisfying'
            WHEN LOWER(commentaire) LIKE '%average product%' THEN 'average'
            WHEN LOWER(commentaire) LIKE '%below average%' THEN 'poor'
            WHEN LOWER(commentaire) LIKE '%delivery took too long%' THEN 'slow'
            WHEN LOWER(commentaire) LIKE '%not satisfied%' THEN 'unsatisfied'
            WHEN LOWER(commentaire) LIKE '%terrible experience%' THEN 'terrible'
            WHEN LOWER(commentaire) LIKE '%great quality%' THEN 'great'
            WHEN LOWER(commentaire) LIKE '%okay%' THEN 'bof'
            WHEN LOWER(commentaire) LIKE '%product arrived damaged%' THEN 'damaged'
            WHEN LOWER(commentaire) LIKE '%perfect experience%' THEN 'perfect'
            WHEN LOWER(commentaire) LIKE '%satisfied with the experience%' THEN 'satisfied'
            WHEN LOWER(commentaire) LIKE '%Customer service was unhelpful%' THEN 'bad'
            ELSE 'bad'
        END AS mots_cles
    FROM base
)
SELECT
    id_commande,
    note_client,
    commentaire,
    sentiment,
    mots_cles
FROM replaced
ORDER BY sentiment
