-- ==============================================================================
-- Vue : v_recurrence_pics_creux (US007)
-- Objectif : Identifier les pics/creux de commandes par jour du mois
-- Méthode : Analyse statistique (z-score) + médiane
-- Adaptée pour affichage du jour de semaine en format européen et nom en français
-- ==============================================================================

WITH commandes_par_jour AS (
  -- Étape 1 : Comptage quotidien des commandes (hors commandes annulées)
  SELECT 
    DATE(date_commande) AS jour,
    EXTRACT(DAY FROM DATE(date_commande)) AS jour_du_mois,
    MOD(EXTRACT(DAYOFWEEK FROM DATE(date_commande)) + 5, 7) + 1 AS jour_de_semaine_fr,
    FORMAT_DATE('%A', DATE(date_commande)) AS nom_jour_en,
    COUNT(*) AS nb_commandes
  FROM {{ ref('mrt_fct_commandes') }}
  WHERE statut_commande != 'Annulée'
  GROUP BY jour, jour_du_mois, jour_de_semaine_fr, nom_jour_en
),

jours AS (
  -- Étape 2 : Dictionnaire de correspondance jour anglais -> français
  SELECT 'Monday' AS en, 'Lundi' AS fr UNION ALL
  SELECT 'Tuesday', 'Mardi' UNION ALL
  SELECT 'Wednesday', 'Mercredi' UNION ALL
  SELECT 'Thursday', 'Jeudi' UNION ALL
  SELECT 'Friday', 'Vendredi' UNION ALL
  SELECT 'Saturday', 'Samedi' UNION ALL
  SELECT 'Sunday', 'Dimanche'
),

stats_par_jour_du_mois AS (
  -- Étape 3 : Statistiques par jour du mois (1 à 31)
  SELECT 
    jour_du_mois,
    APPROX_QUANTILES(nb_commandes, 2)[OFFSET(1)] AS mediane_commandes,
    AVG(nb_commandes) AS moyenne,
    STDDEV(nb_commandes) AS ecart_type
  FROM commandes_par_jour
  GROUP BY jour_du_mois
),

anomalies_par_jour AS (
  -- Étape 4 : Calcul des écarts + z-score
  SELECT 
    c.jour,
    c.jour_du_mois,
    c.jour_de_semaine_fr,
    j.fr AS nom_jour_fr,
    c.nb_commandes,
    s.moyenne,
    s.mediane_commandes,
    s.ecart_type,
    SAFE_DIVIDE(c.nb_commandes - s.moyenne, s.ecart_type) AS z_score,
    CASE 
      WHEN SAFE_DIVIDE(c.nb_commandes - s.moyenne, s.ecart_type) >= 1.5 THEN 'pic'
      WHEN SAFE_DIVIDE(c.nb_commandes - s.moyenne, s.ecart_type) <= -1.5 THEN 'creux'
      ELSE 'normal'
    END AS statut
  FROM commandes_par_jour c
  JOIN stats_par_jour_du_mois s 
    ON c.jour_du_mois = s.jour_du_mois
  LEFT JOIN jours j 
    ON c.nom_jour_en = j.en
)

-- Étape 5 : Résultat enrichi
SELECT 
  jour,
  jour_du_mois,
  jour_de_semaine_fr AS jour_de_semaine,
  nom_jour_fr,
  nb_commandes,
  ROUND(moyenne, 2) AS moyenne_attendue,
  ROUND(mediane_commandes, 2) AS mediane_commandes,
  ROUND(ecart_type, 2) AS ecart_type_jour,
  ROUND(z_score, 2) AS z_score,
  statut
FROM anomalies_par_jour
ORDER BY jour
