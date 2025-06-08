SELECT
    id_commande, 
    date_commande,
    statut_commande,
    quantite,
    prix_unitaire_avant_promotion,
    montant_commande_avant_promotion,
    id_promotion,
    promotion_oui_non,
    type_promotion,
    valeur_promotion,
    montant_commande_apres_promotion

FROM {{ ref('mrt_fct_commandes') }}

WHERE statut_commande != 'Annulée' 

ORDER BY id_commande
