-- Avoir un tableau avec en colonnes : canal ; nombre_de_clics, cout_par_clic, nombre_de_conversions, cout_par_acquisition

SELECT
    id_campagne,
    date, 
    canal,
    SUM(budget) AS budget,
    SUM(clics) AS nombre_de_clics,
    ROUND(SUM(budget) / SUM(clics), 2) AS cout_par_clic,
    SUM(conversions) AS nombre_acquisitions,
    ROUND(SUM(budget) / SUM(conversions), 2) AS cout_par_acquisition
    
FROM {{ ref('mrt_fct_campagnes') }}

GROUP BY id_campagne, date, canal