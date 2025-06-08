-- Analyser la corrélation :
-- Retard estimé : si la livraison est prévue avec un délai supérieur à la médiane globale
-- annee_mois : extraite de la date de livraison estimée
-- Sélectionner uniquement les commandes qui ont été notées (note_client)

WITH delais AS (
  SELECT
    DATE_DIFF(DATE(c.date_livraison_estimee), DATE(c.date_commande), DAY) AS delai
  FROM {{ ref('mrt_fct_commandes') }} c
),

mediane_delai AS (
  SELECT
    PERCENTILE_CONT(delai, 0.5) OVER () AS mediane
  FROM delais
  LIMIT 1
),

commandes_avec_retard AS (
  SELECT
    c.id_commande,
    c.date_commande,
    c.date_livraison_estimee,
    FORMAT_DATE('%Y-%m', DATE(c.date_livraison_estimee)) AS annee_mois,
    DATE_DIFF(DATE(c.date_livraison_estimee), DATE(c.date_commande), DAY) AS delai_commande,
    c.montant_commande_apres_promotion,  -- ✅ Ajout de cette colonne
    m.mediane,
    DATE_DIFF(DATE(c.date_livraison_estimee), DATE(c.date_commande), DAY) > m.mediane AS retard_livraison
  FROM {{ ref('mrt_fct_commandes') }} c
  CROSS JOIN mediane_delai m
)

-- Filtrage final : ne garder que les commandes notées
SELECT
    cwr.*,
    s.note_client,
    s.plainte,
    s.type_plainte
FROM commandes_avec_retard cwr
JOIN {{ ref('mrt_fct_satisfaction') }} s
  ON cwr.id_commande = s.id_commande
