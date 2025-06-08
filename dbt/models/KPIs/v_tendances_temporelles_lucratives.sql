-- =============================================================================
-- Vue : v_tendances_temporelles_lucratives (US006)
-- Objectif : Identifier les jours avec des pics de ventes inhabituels
-- Méthodologie : Z-score mensuel et hebdomadaire sur le CA réel (après promo)
-- =============================================================================

WITH ventes_par_jour AS (
  -- Étape 1 : CA réel par jour (exclure les commandes annulées)
  SELECT
    CAST(f.date_commande AS DATE) AS jour,
    EXTRACT(YEAR FROM CAST(f.date_commande AS DATE)) AS annee,
    EXTRACT(MONTH FROM CAST(f.date_commande AS DATE)) AS mois,
    EXTRACT(WEEK FROM CAST(f.date_commande AS DATE)) AS semaine,
    EXTRACT(DAYOFWEEK FROM CAST(f.date_commande AS DATE)) AS jour_semaine,
    SUM(f.montant_commande_apres_promotion) AS ca_journalier
  FROM {{ ref('mrt_fct_commandes') }} f
  WHERE f.statut_commande != 'Annulée'
  GROUP BY 
    jour, annee, mois, semaine, jour_semaine
),

stats_mensuelles AS (
  -- Étape 2 : Moyenne et écart-type du CA journalier par mois
  SELECT
    mois,
    AVG(ca_journalier) AS moyenne_mois,
    STDDEV(ca_journalier) AS ecart_type_mois
  FROM ventes_par_jour
  GROUP BY mois
),

stats_hebdomadaires AS (
  -- Étape 3 : Moyenne et écart-type par jour de semaine
  SELECT
    jour_semaine,
    AVG(ca_journalier) AS moyenne_jour,
    STDDEV(ca_journalier) AS ecart_type_jour
  FROM ventes_par_jour
  GROUP BY jour_semaine
),

classement_journalier AS (
  -- Étape 4 : Calcul des z-scores
  SELECT
    v.jour,
    v.annee,
    v.mois,
    v.semaine,
    v.jour_semaine,
    v.ca_journalier,

    m.moyenne_mois,
    m.ecart_type_mois,
    SAFE_DIVIDE(v.ca_journalier - m.moyenne_mois, m.ecart_type_mois) AS z_score_mois,

    h.moyenne_jour,
    h.ecart_type_jour,
    SAFE_DIVIDE(v.ca_journalier - h.moyenne_jour, h.ecart_type_jour) AS z_score_jour,

    RANK() OVER (PARTITION BY v.mois ORDER BY v.ca_journalier DESC) AS rang_dans_mois,
    RANK() OVER (PARTITION BY v.jour_semaine ORDER BY v.ca_journalier DESC) AS rang_dans_jour
  FROM ventes_par_jour v
  JOIN stats_mensuelles m ON v.mois = m.mois
  JOIN stats_hebdomadaires h ON v.jour_semaine = h.jour_semaine
)

-- Étape 5 : marquage des jours exceptionnels
SELECT
  jour,
  annee,
  mois,
  semaine,
  jour_semaine,
  ca_journalier,

  ROUND(z_score_mois, 2) AS z_score_mois,
  ROUND(z_score_jour, 2) AS z_score_jour,
  rang_dans_mois,
  rang_dans_jour,

  CASE 
    WHEN z_score_mois >= 1.5 OR z_score_jour >= 1.5 THEN 'pic'
    ELSE 'normal'
  END AS statut_lucratif
FROM classement_journalier
ORDER BY jour
