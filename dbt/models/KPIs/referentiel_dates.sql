-- Avoir un tableau avec tous les référentiels de dates

SELECT
    *
FROM {{ ref('mrt_dim_dates') }}
