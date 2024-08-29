import functions_framework # obligatoire pour l'exe cloud, a commenter en local
import requests
from google.cloud import bigquery
from google.oauth2 import service_account
import google
from unidecode import unidecode

BOOST_URL = "https://www.province-sud.nc/drhouseweb/api/GOUV_WEB/societe/offre_emploi/data?apiKey=***REMOVED***"

# PROD
GCP_PROJECT_ID ="prj-dtefpnc-p-bq-c3bc"
BQ_DATASET = "boost"

# creds = service_account.Credentials.from_service_account_file("prj-dtefpnc-p-bq-c3bc-b13e47749534.json")
creds, _ = google.auth.default()

# Gestion des logs dans cloud run
import google.cloud.logging
logging_client = google.cloud.logging.Client()
logging_client.setup_logging()


def normalize_commune(nom):
    if nom is not None:
        return unidecode(nom).lower()
    else:
        return None


def load():
    data = []
    response = requests.get(BOOST_URL)
    response.raise_for_status()
    res = response.json()
    data.extend(res["data"])
    while True:
        if "hasNextPage" in res and res["paramsNextPageQuery"] is not None:
            url = BOOST_URL+"&id%7CGt="+res["paramsNextPageQuery"]["id|Gt"]
            res = requests.get(url).json()
            data.extend(res["data"])
            for offre in res["data"]:
                if "logiciel" not in offre:
                    print(url)
            if len(data) % 5000 == 0:
                print(len(data))
        else:
            break
        
    return data


def transform(offres):
    print("######## transform ########")
    for offre in offres:
        del offre["logiciel"]
    return offres


def upload(json):
    print("######## upload ########")
    client = bigquery.Client(GCP_PROJECT_ID, credentials=creds,)
    table_id = "%s.%s.%s" % (GCP_PROJECT_ID, BQ_DATASET, 'offres_v2')
    table_ref = bigquery.Table(table_id)
    table = client.create_table(table_ref, exists_ok=True)
    job_config = bigquery.LoadJobConfig(autodetect = True,
                                        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE)    
    job = client.load_table_from_json(json, table, job_config=job_config)
    job.result()
    print(f"Loaded {job.output_rows} rows into {table_id}")

@functions_framework.http
def spe_boost(request):
    offres = load()
    print(len(offres))
    df = transform(offres)
    upload(df)
    return "transfert terminé"


if __name__ == "__main__":
    spe_boost(None)
