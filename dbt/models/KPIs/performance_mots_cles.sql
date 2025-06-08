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
            WHEN LOWER(commentaire) = 'could be better.' THEN 'mediocre'
            WHEN LOWER(commentaire) = 'excellent product highly recommend!' THEN 'excellent'
            WHEN LOWER(commentaire) = 'fast delivery and good service.' THEN 'efficient'
            WHEN LOWER(commentaire) = 'good product, happy with the purchase' THEN 'satisfying'
            WHEN LOWER(commentaire) = 'average product, nothing special' THEN 'average'
            WHEN LOWER(commentaire) = 'below average quality.' THEN 'poor'
            WHEN LOWER(commentaire) = 'delivery took too long.' THEN 'slow'
            WHEN LOWER(commentaire) = 'not satisfied with the service.' THEN 'unsatisfied'
            WHEN LOWER(commentaire) = 'terrible experience, will not buy again.' THEN 'terrible'
            WHEN LOWER(commentaire) = 'great quality and service!' THEN 'great'
            WHEN LOWER(commentaire) = 'it was okay, not great.' THEN 'bof'
            WHEN LOWER(commentaire) = 'the product arrived damaged.' THEN 'damaged'
            WHEN LOWER(commentaire) = 'perfect experience, very happy!' THEN 'perfect'
            WHEN LOWER(commentaire) = 'satisfied with the experience.' THEN 'satisfied'
            ELSE NULL
        END AS mots_cles
    FROM base
),

final AS (
    SELECT
        id_commande,
        commentaire,
        note_client,
        sentiment,
        CASE
            WHEN note_client = 5 AND (mots_cles IS NULL OR mots_cles = '') THEN 'perfect'
            WHEN note_client = 3 AND (mots_cles IS NULL OR mots_cles = '') THEN 'mediocre'
            ELSE mots_cles
        END AS mots_cles
    FROM replaced
),

freq AS (
    SELECT
        mots_cles,
        COUNT(*) AS frequence
    FROM final
    GROUP BY mots_cles
)

SELECT
    f.id_commande,
    f.commentaire,
    f.sentiment,
    f.mots_cles,
    fr.frequence
FROM final f
LEFT JOIN freq fr
ON f.mots_cles = fr.mots_cles
ORDER BY fr.frequence DESC
