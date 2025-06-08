from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from datetime import timedelta # utile pour les retries si erreur 
import gspread
from google.oauth2 import service_account
import pandas as pd
import re
from googleapiclient.discovery import build #utilisé pour gérer les fichiers .xlsx
from googleapiclient.http import MediaIoBaseDownload #utilisé pour gérer les fichiers .xlsx
import io #utilisé pour gérer les fichiers .xlsx
from pandas_gbq import to_gbq
import time # utilisé pour gérer un time sleep qui évitera de surcharger les APIs Gsheets et Gdrive 
import requests # utilisé pour exécuter le job dbt 

# Variables Gsheets et Gdrive pour assurer l'accès à ces services
scope = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

# Chemin d'accès vers la clef json qui permet d'ouvrir les documents sources 
json_key = "/home/analyst/airflow/Json_keys/json_key_gsheets_to_airflow_to_bigquery.json"

# Stockage des ids des 10 documents sources 
campagnes_id = "1_WxFdSWGGCNreMgSWf9nfuP-Ye_RnCX1Xs5ubnjGp9s"
clients_id = "1PkZuSLHn0eZQLjhBx8qdZ_bh_wzgMbenrYyMGYrxBic"
commandes_id = "1QVXmhf9b2OSpUVb7uBOQOClk19ldleNYQcloKCrHlgA"
details_commandes_id = "1kN4O2D-LIvbLSTse2RsguJMPwdMWKtVY6dEl_4hcyqw"
entrepots_machines_id = "1s9R6eJPlC0Vwz_OPRTZ43XXfknBAXktn"
entrepots_id = "1FSP2Gv31H1lnpLh6nmaNFcKlCE11OlbA"
posts_id = "1N81drG9zhp9VBZh3LqPoQ01cMvXol1kX43hqhQtAZ44"
produits_id = "1I4KHaFSEMMJ2E7OEO-v1KWbYfOGUBGiC8XCUVvFHs2I"
promotions_id = "1p2O-Zgmhcmfov1BkLb7Rx9k2iwg65kFcgVyYwb4CYs4"
satisfaction_id = "1G7rST778z_zcewJX9CuURwIqTSKfWCU_i6ZJ9P8edzM"

# J'initialise le "client" qui me servira à authentifier l'accès aux Gsheets, cette fonction sera factorisée
 
def get_client():
    
    creds = service_account.Credentials.from_service_account_file(json_key, scopes=scope)
    client = gspread.authorize(creds)
    return client

# Je crée une fonction de nettoyage du nom des colonnes, cette fonction sera elle-aussi factorisée 

def clean_column_names(df):

    def replace_accents(nom_initial): # dans cette sous fonction, je remplace les noms initiaux des colonnes, par des nouveaux noms qui suivent une nomencalture propre 
        return re.sub(
            r'[ôâéèêÔÂÉÈÊ]', # je définis ce pattern de caractères non-désiré à remplacer par son équivalent propre, via le re.sub 
            lambda x: { # Je définis une lambda qui s'appliquera à chaque caractère non-désiré
                'ô':'o', # je remplace les ô par des o et ainsi de suite 
                'â':'a',
                'é':'e',
                'è':'e',
                'ê':'e',
                'Ô':'O',
                'Â':'A',
                'É':'E',
                'È':'E',
                'Ê':'E'
            }[x.group()], # retourne le caractère non-désiré en question, repéré par la RegEx 
            nom_initial # remplace le caractère non-désiré, par le caractère propre, dans le texte initial qui est nom_initial 
        )

    def clean(col):
        col = replace_accents(col)
        col = col.lower()
        col = re.sub(r'[ \-]+', '_', col) # espaces et tirets remplacés par _
        col = re.sub(r'[^a-z0-9_]', '', col) # supprimer tout sauf a-z, 0-9, _
        return col

    df.columns = [clean(col) for col in df.columns]
    return df

# Je crée une fonction de dédoublonnage pour les ids, qui ne me gardera que les lignes non nulles et la ligne relative à la première valeur de chaque occurrence, cette fonction sera factorisée 

def dedoublonnage_id(df, colonnes):
    for col in colonnes:
        df = df[df[col].notnull()]
    return df.drop_duplicates(subset=colonnes, keep='first')

# Je créer une fonctione de remplissage des valeurs manquantes, qui remplacera les valeurs manquantes par un 0, cette fonction sera factorisée 

def remplissage(df, colonnes):
    df[colonnes] = df[colonnes].fillna(0)
    return df

# Je crée une fonction de filtre, qui ne gardera que les lignes dont la valeur du champ spécifié, sera supérieur ou égale à 0, cette fonction sera factorisée 

def filtrage(df, colonnes):
    for col in colonnes:
        df = df[df[col] >= 0]
    return df

# Je crée une fonction d'envoi des dataframes à big query 

def push_df_to_bigquery(df, table_name):
    to_gbq(
        dataframe=df,
        destination_table=table_name,
        project_id='carttrend-460508',
        if_exists='replace',
        credentials=service_account.Credentials.from_service_account_file(json_key)
    )

# Enregistremeent des paramètres de connexion à dbt cloud pour que le DAG puisse exécuter et monitorer le job de transformation en production 

dbt_api_token = "dbtu_T_HNX_YGaHjqg3wc9MAu5F7a67tDz0jimgwM14MR2v-d5zQbgA" # jeton d'api pour pouvoir déclencher le job à distance 
dbt_account_id = "70471823462994" # le compte en question, qui inclue le projet 
dbt_job_id = "70471823467147" # job qui exécute le test et la transformation des modèles 

# Je crée une fonction de déclenchement du job dbt cloud qui sert au test des modèles et à leur transformation puis réinjection dans le mart 

def run_dbt_transform():
    headers = {
        'Authorization': f'Token {dbt_api_token}',
        'Content-Type': 'application/json'
    }

    # Lancer le job DBT 
    data = {"cause": "Exécution automatique via Airflow"}
    run_url = f"https://pj519.us1.dbt.com/api/v2/accounts/{dbt_account_id}/jobs/{dbt_job_id}/run/"

    response = requests.post(run_url, headers=headers, json=data)

    if response.status_code == 200:
        run_id = response.json()['data']['id']
        print(f"Job DBT Cloud lancé avec succès. Run ID: {run_id}")
        return run_id
    else:
        print(f"Erreur lors du lancement du job DBT Cloud : {response.text}")
        return None

# Initialisation des paramètres par défaut du DAG

default_args ={
    "owner":"airflow",
    "retries":1,
    "retry_delay": timedelta(seconds=10),
    "email_on_failure": True, # Active mail en cas d’échec
    "email_on_retry": False, # Optionnel : mail en cas de retry
    "email" : ["lacapsule.carttrend@gmail.com"],  # Liste des emails à notifier
}

# ------------------------------------ ETAPE I : Collecte des données Gsheets ----------------------------------------------------------

# GET GSHEETS: réalisation d'une tâche GET pour collecter les données des 8 fichiers Gsheets suivants : campagnes, clients, commandes, details_commandes, posts, produits, promotions et satisfaction 

def task_get_gsheets_data(ti, **kwargs):
    client = get_client() #Authentification Gsheets
    
    sheets_info = {
        'df_campagnes': campagnes_id,
        'df_clients': clients_id,
        'df_commandes': commandes_id,
        'df_details_commandes': details_commandes_id,
        'df_posts': posts_id,
        'df_produits': produits_id,
        'df_promotions': promotions_id,
        'df_satisfaction': satisfaction_id,
    }

    for key, sheet_id in sheets_info.items():
        df = pd.DataFrame(client.open_by_key(sheet_id).sheet1.get_all_records())
        ti.xcom_push(key=key, value=df.to_json(orient='records'))
        time.sleep(1) # Ajout d'un sleep d'1 seconde, afin de ne pas surcharger les appels vers Gsheets
        print(f"{key}\n{df.head(1)}") # j'affiche la première ligne histoire de pouvoir visualiser dans les logs AIrflow, si le get a bien marché 

# ------------------------------------ ETAPE II : Collecte des données XLSX ----------------------------------------------------------

# GET XLSX : réalisation d'une autre tâche GET, pour collecter les données des 2 fichiers XLSX suivants : entrepots et entrepots_machines 

def task_get_xlsx_data(**context):

    # Authentification 
    creds = service_account.Credentials.from_service_account_file(json_key, scopes=scope)
    drive_service = build('drive', 'v3', credentials=creds)

    # Dictionnaire contenant les noms de variables et les IDs des fichiers à récupérer
    xlsx_info = {
        'df_entrepots_machines': entrepots_machines_id,
        'df_entrepots': entrepots_id,
    }

    # Boucle sur les 2 fichiers à télécharger et charger
    for key, file_id in xlsx_info.items():
        file = io.BytesIO()  # Crée un espace pour mémoriser le contenu du fichier

        # Prépare la requête de téléchargement
        request = drive_service.files().get_media(fileId=file_id)
        downloader = MediaIoBaseDownload(file, request)

        # Télécharge le fichier en plusieurs morceaux si nécessaire
        done = False
        while done is False:
            status, done = downloader.next_chunk() #tant que le statut indique que le téléchargement n'est pas terminé, alors je télécharge le prochain "chunk" ou portion de données

        file.seek(0) # Repositionne le curseur au début du fichier téléchargé pour le prochain téléchargement 

        # Lit le fichier Excel en DataFrame
        df = pd.read_excel(file)

        # Envoie la data dans XCom pour le rendre disponible aux autres tâches
        context['ti'].xcom_push(key=key, value=df.to_json(orient='records'))
        print(f"Debug: Pushed data for key: {key}")  # Log pour vérifier que les données sont bien poussées
        time.sleep(1) # Ajout d'un sleep d'1 seconde, afin de ne pas surcharger les appels vers Gdrive
        print(f"{key}\n{df.head(1)}") # j'affiche la première ligne histoire de pouvoir visualiser dans les logs Airflow, si le get a bien marché 

# ------------------------------------ ETAPE III : Prétransformation des 10 dataframes et envoi vers Big Query ----------------------------------------------------------

def task_pretransform_load_data(**context):
    ti = context['ti']

    # Dictionnaire des clés XCom et leurs tâches sources
    json_data = {
        'df_campagnes': 'get_gsheets_data',
        'df_clients': 'get_gsheets_data',
        'df_commandes': 'get_gsheets_data',
        'df_details_commandes': 'get_gsheets_data',
        'df_posts': 'get_gsheets_data',
        'df_produits': 'get_gsheets_data',
        'df_promotions': 'get_gsheets_data',
        'df_satisfaction': 'get_gsheets_data',
        'df_entrepots_machines': 'get_xlsx_data',
        'df_entrepots': 'get_xlsx_data',
    }

    dataframes = {}

    # Récupération des données depuis le Xcom, nettoyage des noms des colonnes
    for df_key, task_id in json_data.items():
        df_json = ti.xcom_pull(key=df_key, task_ids=task_id)
        print(f"Debug: Retrieved JSON for {df_key}: {df_json}")  # Log pour vérifier la valeur récupérée
        if df_json is None:
            raise ValueError(f"No data found for key: {df_key}")
        df = pd.read_json(df_json, orient='records')
        df = clean_column_names(df)  # nettoyage des noms de cols de tous les dataframes
        dataframes[df_key] = df # je stocke le dataframe, dans une liste de dataframes

    # (1/10) CAMPAGNES : Prétransformation  
    df_campagnes = dataframes['df_campagnes']
    df_campagnes = dedoublonnage_id(df_campagnes, ['id_campagne']) # Dédoublonnage des ids et suppression lignes nulles
    df_campagnes = filtrage(df_campagnes, ['budget', 'impressions', 'clics', 'conversions']) # Filtrage pour ne garder que les valeurs supérieures ou égales à 0 
    print(df_campagnes.head(1))
    push_df_to_bigquery(df_campagnes, 'dataset_airflow.campagnes') # Envoyer le dataframe vers une table BigQuery

    # (2/10) CLIENTS : prétransformation 
    df_clients = dataframes['df_clients']
    df_clients = dedoublonnage_id(df_clients, ['id_client']) # Dédoublonnage des ids et suppression lignes nulles
    print(df_clients.head(1))
    push_df_to_bigquery(df_clients, 'dataset_airflow.clients') # Envoyer le dataframe vers une table BigQuery

    # (3/10) COMMANDES : Prétransformation  
    df_commandes = dataframes['df_commandes']
    df_commandes = dedoublonnage_id(df_commandes, ['id_commande']) # Dédoublonnage des ids et suppression lignes nulles
    print(df_commandes.head(1))
    push_df_to_bigquery(df_commandes, 'dataset_airflow.commandes') # Envoyer le dataframe vers une table BigQuery

    # (4/10) DETAILS_COMMANDES : Prétransformation 
    df_details_commandes = dataframes['df_details_commandes'] 
    df_details_commandes = dedoublonnage_id(df_details_commandes, ['id_commande']) # Dédoublonnage des ids et suppression lignes nulles
    df_details_commandes = filtrage(df_details_commandes, ['quantite']) # Suppression des valeurs aberrantes négatives
    df_details_commandes = df_details_commandes[df_details_commandes['id_produit'].notnull()] # je ne garde que les lignes qui ont un id_produit complété 
    print(df_details_commandes.head(1))
    push_df_to_bigquery(df_details_commandes, 'dataset_airflow.details_commandes') # Envoyer le dataframe vers une table BigQuery


    # (5/10) ENTREPOTS_MACHINES : prétransformation
    df_entrepots_machines = dataframes['df_entrepots_machines'] 
    df_entrepots_machines = dedoublonnage_id(df_entrepots_machines, ['id']) # Dédoublonnage des ids et suppression lignes nulles
    df_entrepots_machines = remplissage(df_entrepots_machines,['temps_darret','volume_traite']) # Remplacer le temps_darret et volume_traite par des 0 si valeurs manquantes 
    print(df_entrepots_machines.head(1))
    push_df_to_bigquery(df_entrepots_machines, 'dataset_airflow.entrepots_machines') # Envoyer le dataframe vers une table BigQuery

    # (6/10) ENTREPOTS : prétransformation 
    df_entrepots = dataframes['df_entrepots'] 
    df_entrepots = dedoublonnage_id(df_entrepots, ['id_entrepot']) # Dedoublonnage id et sélection ids non nuls
    print(df_entrepots.head(1))
    push_df_to_bigquery(df_entrepots, 'dataset_airflow.entrepots') # Envoyer le dataframe vers une table BigQuery

    # (7/10) POSTS : Prétransformation 
    df_posts = dataframes['df_posts'] 
    df_posts = dedoublonnage_id(df_posts,['id_post']) # Dedoublonnage id et sélection ids non nuls
    df_posts = filtrage(df_posts,['volume_mentions']) # Filtrage des liges dont volume_mentions est supérieur ou égal à 0 
    print(df_posts.head(1))
    push_df_to_bigquery(df_posts, 'dataset_airflow.posts') # Envoyer le dataframe vers une table BigQuery

    # (8/10) PRODUITS : Prétransformation  
    df_produits = dataframes['df_produits'] 
    df_produits = dedoublonnage_id(df_produits,['id']) # Dedoublonnage id et sélection ids non nuls
    df_produits = filtrage(df_produits,['prix']) # Sélection des lignes dont le prix est supérieur ou égal à 0 
    print(df_produits.head(1))
    push_df_to_bigquery(df_produits, 'dataset_airflow.produits') # Envoyer le dataframe vers une table BigQuery

    # (9/10) PROMOTIONS : Prétransformation 
    df_promotions = dataframes['df_promotions'] 
    df_promotions = dedoublonnage_id(df_promotions,['id_promotion']) # Dedoublonnage id et sélection ids non nuls 
    print(df_promotions.head(1)) 
    push_df_to_bigquery(df_promotions, 'dataset_airflow.promotions') # Envoyer le dataframe vers une table BigQuery

    # (10/10) SATISFACTION : Prétransformation 
    df_satisfaction = dataframes['df_satisfaction'] 
    df_satisfaction = dedoublonnage_id(df_satisfaction, ['id_satisfaction']) # Dedoublonnage id et sélection ids non nuls
    df_satisfaction = remplissage(df_satisfaction,['temps_reponse_support']) # Remplacer le temps_reponse_support par des 0 si valeurs manquantes
    print(df_satisfaction.head(1)) 
    push_df_to_bigquery(df_satisfaction, 'dataset_airflow.satisfaction') # Envoyer le dataframe vers une table BigQuery

# ------------------------------------ DEFINITION DU DAG ----------------------------------------------------------

with DAG(
    dag_id='projet_dag_11',
    default_args=default_args,
    start_date=datetime(2025, 5, 22),
    schedule_interval="0 5 * * *",  # Exécution tous les jours à 5h du matin
    catchup=False
) as dag:

    get_gsheets_data = PythonOperator(
        task_id='get_gsheets_data',
        python_callable=task_get_gsheets_data,
    )

    get_xlsx_data = PythonOperator(
        task_id='get_xlsx_data',
        python_callable=task_get_xlsx_data,
    )

    pretransform_load_data = PythonOperator(
        task_id='pretransform_load_data',
        python_callable=task_pretransform_load_data,
    )

    transform_data = PythonOperator(
        task_id='run_dbt_transform',
        python_callable=run_dbt_transform, 
    )

    get_gsheets_data >> get_xlsx_data >> pretransform_load_data >> transform_data
