-- =============================================================================
-- Vue : v_recurrence_pics_creux_semaines
-- Objectif : Détecter les semaines lucratives avec z-score ISO, en ne gardant que les champs nécessaires
-- =============================================================================

WITH ventes_par_jour AS (
  SELECT
    FORMAT_TIMESTAMP('%G', f.date_commande) AS annee_iso,
    FORMAT_TIMESTAMP('%V', f.date_commande) AS semaine_iso,
    FORMAT_TIMESTAMP('%G-S%V', f.date_commande) AS semaine_affichee,
    EXTRACT(MONTH FROM f.date_commande) AS mois,
    EXTRACT(DAYOFWEEK FROM f.date_commande) AS jour_semaine,
    SUM(f.montant_commande_apres_promotion) AS ca_journalier
  FROM `carttrend-460508.dev_sbeghin.mrt_fct_commandes` f
  WHERE f.statut_commande != 'Annulée'
  GROUP BY annee_iso, semaine_iso, semaine_affichee, mois, jour_semaine
),

stats_mensuelles AS (
  SELECT
    mois,
    AVG(ca_journalier) AS moyenne_mois,
    STDDEV(ca_journalier) AS ecart_type_mois
  FROM ventes_par_jour
  GROUP BY mois
),

stats_hebdomadaires AS (
  SELECT
    jour_semaine,
    AVG(ca_journalier) AS moyenne_jour,
    STDDEV(ca_journalier) AS ecart_type_jour
  FROM ventes_par_jour
  GROUP BY jour_semaine
),

classement_journalier AS (
  SELECT
    v.annee_iso,
    v.semaine_iso,
    v.semaine_affichee,
    v.ca_journalier,
    SAFE_DIVIDE(v.ca_journalier - m.moyenne_mois, m.ecart_type_mois) AS z_score_mois,
    SAFE_DIVIDE(v.ca_journalier - h.moyenne_jour, h.ecart_type_jour) AS z_score_jour,
    CASE 
      WHEN SAFE_DIVIDE(v.ca_journalier - m.moyenne_mois, m.ecart_type_mois) >= 1.5 
           OR SAFE_DIVIDE(v.ca_journalier - h.moyenne_jour, h.ecart_type_jour) >= 1.5 
      THEN 'pic'
      ELSE 'normal'
    END AS statut_lucratif_jour
  FROM ventes_par_jour v
  JOIN stats_mensuelles m ON v.mois = m.mois
  JOIN stats_hebdomadaires h ON v.jour_semaine = h.jour_semaine
),

semaines_lucratives AS (
  SELECT
    annee_iso AS annee,
    semaine_iso AS semaine,
    semaine_affichee,
    SUM(ca_journalier) AS ca_total_semaine,
    SUM(CASE WHEN statut_lucratif_jour = 'pic' THEN 1 ELSE 0 END) AS nb_jours_lucratifs,
    CASE 
      WHEN SUM(CASE WHEN statut_lucratif_jour = 'pic' THEN 1 ELSE 0 END) >= 3 
      THEN 'Semaine lucrative'
      ELSE 'Semaine normale'
    END AS statut_lucratif_semaine
  FROM classement_journalier
  GROUP BY annee_iso, semaine_iso, semaine_affichee
)

SELECT *
FROM semaines_lucratives
ORDER BY annee, semaine
