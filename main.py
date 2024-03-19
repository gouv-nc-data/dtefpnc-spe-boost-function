import functions_framework # obligatoire pour l'exe cloud, a commenter en local
import requests
from google.cloud import bigquery
from google.oauth2 import service_account
import google
from unidecode import unidecode

BOOST_URL = "https://www.province-sud.nc/searchweb/searchweb/DocSolr?classNaturalName=Offre%20d%27emploi%20%2F%20Stage&limit=2000&page=0&start=0&_responseMode=json"

# PROD
GCP_PROJECT_ID ="prj-dtefpnc-p-bq-c3bc"
BQ_DATASET = "boost"

# creds = service_account.Credentials.from_service_account_file("prj-dtefpnc-p-bq-c3bc-4674a1c473e9.json")
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
    print("######## load ########")
    response = requests.get(BOOST_URL)
    response.raise_for_status()
    return response.json()["data"]


def transform(offres):
    print("######## transform ########")
    print(offres[:5])
    for offre in offres:
        offre["highlighting"] = None if offre["highlighting"] == {} else offre["highlighting"]
        offre["communeLocalisation"] = normalize_commune(offre["communeLocalisation"])
    return offres


def upload(json):
    print("######## upload ########")
    client = bigquery.Client(GCP_PROJECT_ID, credentials=creds,)
    table_id = "%s.%s.%s" % (GCP_PROJECT_ID, BQ_DATASET, 'boost')
    table_ref = bigquery.Table(table_id)
    table = client.create_table(table_ref, exists_ok=True)
    job_config = bigquery.LoadJobConfig(autodetect = True,
                                        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE)    
    job = client.load_table_from_dataframe(json, table, job_config=job_config)
    job.result()
    print(f"Loaded {job.output_rows} rows into {table_id}")

@functions_framework.http
def spe_boost(request):
    offres = load()
    df = transform(offres)
    upload(df)
    return "transfert terminé"


if __name__ == "__main__":
    spe_boost(None)
