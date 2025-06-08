# Carttrend – Projet Data & Machine Learning

Ce dépôt contient l’ensemble des livrables du projet de fin de formation Data Analyst, organisé autour d’un pipeline ELT, de modélisations dbt, de visualisations Power BI et de notebooks analytiques.

## 📁 Arborescence du dépôt

```
carttrend-data-project/
├── dags/                       # DAG Airflow pour l'orchestration ETL
│   └── dag_elt_carttrend.py
│
├── dbt/                        # Projet dbt pour la transformation dans BigQuery
│   ├── models/
│   └── dbt_project.yml
│
├── data_sources/              # Données brutes extraites de Google Sheets et Excel
│   ├── clients.xlsx
│   ├── commandes.xlsx
│   └── ...
│
├── notebooks/                 # Notebooks Jupyter pour l’analyse exploratoire et le ML
│   ├── eda_ventes.ipynb
│   └── xgboost_prevision_ventes.ipynb
│
├── powerbi/                   # Captures et exports des tableaux de bord Power BI
│   ├── dashboard_ventes.png
│   └── dashboard_marketing.pbix
│
├── scripts/                   # Fonctions Python réutilisables (nettoyage, prépa, etc.)
│   └── nettoyage.py
│
├── docs/                      # Documents livrables (rapport, grille, annexes)
│   ├── rapport_carttrend.docx
│   ├── grille_evaluation.pdf
│   └── complements.docx
│
├── requirements.txt           # Librairies nécessaires à l'exécution du projet
└── README.md                  # Présentation du projet (ce fichier)
```

## 🧠 Technologies clés
- Python, Pandas, scikit-learn, XGBoost
- Apache Airflow
- Google BigQuery & dbt
- Power BI
- Google Sheets API, Drive API

## 📊 Objectifs du projet
- Intégrer, nettoyer et modéliser des données multi-sources
- Mettre en place un pipeline de traitement automatisé (Airflow → BigQuery)
- Réaliser des analyses prédictives (XGBoost) et des recommandations métiers
- Créer des tableaux de bord interactifs pour la prise de décision

## 📝 Auteur
Projet réalisé dans le cadre de la formation Data Analyst & IA – La Capsule  
Par : Kahina M., Jean-Laurent V. et Setareh B.
