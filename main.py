import functions_framework # obligatoire pour l'exe cloud, a commenter en local
import requests
from google.cloud import bigquery
from google.oauth2 import service_account
import google

BOOST_URL = "https://www.province-sud.nc/searchweb/searchweb/DocSolr?classNaturalName=Offre%20d%27emploi%20%2F%20Stage&limit=2000&page=0&start=0&_responseMode=json"

# PROD
GCP_PROJECT_ID ="prj-dtefpnc-p-bq-c3bc"
BQ_DATASET = "spe"

# creds = service_account.Credentials.from_service_account_file("prj-dtefpnc-p-bq-c3bc-4674a1c473e9.json")
creds, _ = google.auth.default()

# Gestion des logs dans cloud run
import google.cloud.logging
logging_client = google.cloud.logging.Client()
logging_client.setup_logging()


def load():
    print("######## load ########")
    response = requests.get(BOOST_URL)
    response.raise_for_status()
    return response.json()["data"]


def transform(offres):
    print("######## transform ########")
    for offre in offres:
        offre["highlighting"] = None if offre["highlighting"] == {} else offre["highlighting"]
    return offres


def upload(json):
    print("######## upload ########")
    client = bigquery.Client(GCP_PROJECT_ID, credentials=creds,)
    table_id = "%s.%s.%s" % (GCP_PROJECT_ID, BQ_DATASET, 'boost')
    table_ref = bigquery.Table(table_id)
    table = client.create_table(table_ref, exists_ok=True)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.autodetect = True
    job = client.load_table_from_json(json, table)
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
