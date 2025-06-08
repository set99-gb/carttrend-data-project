-- ============================================================================
-- Modèle dbt : v_indicateurs_synthese
-- Objectif : Synthétiser les indicateurs clés de performance commerciale et logistique
-- Méthode : Agrégations globales sur les commandes et les ressources
-- ============================================================================

{{
  config(
    materialized='view'
  )
}}

-- Étape 1 : Préparer les bases
WITH base_commandes AS (
  SELECT *
  FROM {{ ref('mrt_fct_commandes') }}
),

base_machines AS (
  SELECT *
  FROM {{ ref('mrt_fct_machines') }}
),

-- Étape 2 : Indicateurs agrégés sur les commandes
indicateurs_commandes AS (
  SELECT
    -- Ligne 1 : Impact des annulations
    ROUND(SUM(CASE WHEN statut_commande != 'Annulée' THEN montant_commande_apres_promotion ELSE 0 END)) AS ca_effectif,
    ROUND(SUM(CASE WHEN statut_commande = 'Annulée' THEN montant_commande_apres_promotion ELSE 0 END)) AS ca_annule,
    ROUND(SUM(montant_commande_apres_promotion)) AS ca_total_si_pas_annulation,

    -- Ligne 2 : Impact des promotions
    ROUND(SUM(CASE WHEN statut_commande != 'Annulée' THEN montant_commande_apres_promotion ELSE 0 END)) AS ca_effectif_apres_promo,
    ROUND(SUM(CASE WHEN statut_commande != 'Annulée' THEN montant_commande_avant_promotion - montant_commande_apres_promotion ELSE 0 END)) AS montant_promotions,
    ROUND(SUM(CASE WHEN statut_commande != 'Annulée' THEN montant_commande_avant_promotion ELSE 0 END)) AS ca_sans_promotion,

    -- Autres indicateurs
    ROUND(SUM(CASE WHEN statut_commande = 'Annulée' THEN montant_commande_avant_promotion ELSE 0 END)) AS ca_theorique_annule,
    COUNTIF(statut_commande != 'Annulée') AS volume_commandes,
    COUNTIF(statut_commande = 'Annulée') AS volume_annule,
    COUNTIF(NOT statut_commande = 'Annulée' AND id_promotion IS NOT NULL AND TRIM(id_promotion) != '') AS volume_avec_promotion,
    ROUND(SUM(CASE WHEN statut_commande = 'Annulée' THEN montant_commande_apres_promotion ELSE 0 END)) AS montant_annule,
    ROUND(SUM(CASE WHEN statut_commande != 'Annulée' AND id_promotion IS NOT NULL AND TRIM(id_promotion) != '' THEN montant_commande_apres_promotion ELSE 0 END)) AS montant_avec_promotion,
    COUNTIF(statut_commande = 'Annulée') * 100.0 / COUNT(*) AS pourcentage_annulation,
    ROUND((1 - SUM(CASE WHEN statut_commande != 'Annulée' THEN montant_commande_apres_promotion ELSE 0 END) /
              SUM(CASE WHEN statut_commande != 'Annulée' THEN montant_commande_avant_promotion ELSE NULL END)) * 100, 2) AS taux_promotion
  FROM base_commandes
),

-- Étape 3 : Indicateurs logistiques
indicateurs_opportunite AS (
  SELECT
    (SELECT COUNT(DISTINCT id_entrepot_depart) FROM base_commandes) AS nb_entrepots,
    (SELECT COUNT(DISTINCT id_entrepot_machine) FROM base_machines) AS nb_machines
)

-- Étape 4 : Exposition finale
SELECT *
FROM indicateurs_commandes,
     indicateurs_opportunite
