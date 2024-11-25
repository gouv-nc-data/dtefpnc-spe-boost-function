import requests, os, logging, time
from google.cloud import bigquery
from google.oauth2 import service_account
import google
from unidecode import unidecode

import functions_framework
import google.cloud.logging


#----------------------------
# Context
#----------------------------
BOOST_TOKEN = os.environ['apikey-boost']
BOOST_TABLE = "olap_offre_emploi_log"
BOOST_URL = f"https://www.province-sud.nc/drhouseweb/api/GOUV_WEB/societe/{BOOST_TABLE}/data?apiKey={BOOST_TOKEN}"

# PROD
GCP_PROJECT_ID ="prj-dtefpnc-p-bq-c3bc"
BQ_DATASET = "boost"
BQ_TABLE = "offres_v3"

cred_file = 'prj-dtefpnc-p-bq-c3bc-311cdf7d3088.json'
env_local = os.path.exists(cred_file)
if env_local:
    creds = service_account.Credentials.from_service_account_file(cred_file)
else:
    creds, _ = google.auth.default()

    # Gestion des logs dans cloud run
    logging_client = google.cloud.logging.Client()
    logging_client.setup_logging()

client = bigquery.Client(GCP_PROJECT_ID, credentials=creds,)

#----------------------------
# Fonctions
#----------------------------
def normalize_commune(nom):
    if nom is not None:
        return unidecode(nom).lower()
    else:
        return None

def get_last_update_date(bq_client):
    try:
        query = f"SELECT max(_operation_date) last_update FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`"
        query_job = bq_client.query(query)
        
        for row in query_job.result():
            last_update = row.last_update

        if last_update:            
            # Convertir au format Boost
            boost_format_date = last_update.strftime('%Y-%m-%d')
            logging.debug(f"Date formatée pour Boost: {boost_format_date}")
        else:
            logging.warning("Aucune date récupérée")
            return None

        return boost_format_date
    
    except Exception as e:
        logging.error(f"Une erreur s'est produite : {e}")
        
        return None

def fetch_offers(start_date=None,start_nb=0):
    data = []

    if not start_date:
        start_date = "2000-01-01"

    while True:
        params = f"&_operation_date|datetimeGt={start_date}&_order=_operation_date&start={start_nb}"
        url = BOOST_URL + params
        
        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                response = requests.get(url)
                response.raise_for_status()
                break
            except requests.exceptions.HTTPError as e:
                if response.status_code == 500 and attempt < max_retries:
                    logging.warning(f"Tentative {attempt} échouée, nouvelle tentative dans 3 secondes...")
                    time.sleep(3)
                else:
                    raise  # Relancer l'exception si on est à la dernière tentative
    
        try:
            res = response.json()
        except ValueError as e:
            logging.error(f"Erreur lors de l'extraction du JSON : {e}")
            logging.error(f"Erreur de parse, reponse : {response.text}")

        data.extend(res["data"])

        for offre in res["data"]:
            if "logiciel" not in offre: # Ca devait faire planter le traitement ?
                logging.info(url)

        if len(data) % 5000 == 0:
            logging.info(f'{len(data)} lignes récoltées jusqu\'à présent.')

        if not res["hasNextPage"] or res["paramsNextPageQuery"] is None:
            break

        start_nb = res["paramsNextPageQuery"]["start"]

    return data


def transform(offres):
    logging.info("######## transform ########")
    for offre in offres:
        if isinstance(offre, dict):
            del offre["logiciel"]
        else:
            logging.error(f"Error: Expected a dict but got : {type(offre)}")
            logging.info(f"Offre : {offre}")
    return offres


def upload(json, client_bq, table_name):
    logging.info("######## upload ########")
    
    table_id = "%s.%s.%s" % (GCP_PROJECT_ID, BQ_DATASET, table_name)
    table_ref = bigquery.Table(table_id)
    table = client_bq.create_table(table_ref, exists_ok=True)
    job_config = bigquery.LoadJobConfig(autodetect = True,
                                        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE)    
    job = client_bq.load_table_from_json(json, table, job_config=job_config)
    job.result()
    logging.info(f"Loaded {job.output_rows} rows into {table_id}")

def merge_tables(bq_client, target, source):
    delete_query  = f"""
    DELETE FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{target}`
    WHERE id IN (
    SELECT id FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{source}`
    )
    """
    bq_client.query(delete_query)
    insert_query = f"""
    INSERT INTO `{GCP_PROJECT_ID}.{BQ_DATASET}.{target}`
    SELECT * FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{source}`
    """
    bq_client.query(insert_query)

    bq_client.delete_table(f'{GCP_PROJECT_ID}.{BQ_DATASET}.{source}')
    logging.info(f"Table temporaire {source} supprimée.")

def clean_table(bq_client, table_name):
    # supprime les lignes pour lesquelles le id n'est pas unique en gardant celle avec la date la plus proche
    delete_query = f"""
    DELETE FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{table_name}`
    WHERE STRUCT(id, _operation_date) NOT IN (
    SELECT AS STRUCT id, MAX(_operation_date) AS _operation_date
    FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{table_name}`
    GROUP BY id
    )
    """
    bq_client.query(delete_query)
    logging.info("Table nettoyée des lignes pas à jour")

@functions_framework.http
def spe_boost(request):
    table_name = BQ_TABLE
    boost_format_date = get_last_update_date(client)

    if boost_format_date:
        initial_table = table_name
        table_name = '_tmp_' + table_name

    offres = fetch_offers(boost_format_date)
    logging.info(f"Nouvelles offres : {len(offres)}")
    json = transform(offres)

    if not len(json) == 0:
        upload(json, client, table_name)
        
        if boost_format_date:
            merge_tables(client, initial_table, table_name)
            table_name = initial_table # pour le clean
            
        clean_table(client, table_name)
    return "transfert terminé"


if __name__ == "__main__":
    spe_boost(None)
