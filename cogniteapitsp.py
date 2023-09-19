import os

from cognite.client import CogniteClient, ClientConfig
from cognite.client.credentials import OAuthClientCredentials

CDF_CLUSTER = "westeurope-1"

TENANT_ID = os.getenv("TENANT_ID_TECHBYCOGNITE")
CLIENT_ID = os.getenv("CLIENT_ID_TECHBYCOGNITE_PYTHON_SDK")
CLIENT_SECRET = os.getenv("CLIENT_SECRET_TECHBYCOGNITE_PYTHON_SDK")

COGNITE_PROJECT = "tech-sales-prototyping"

SCOPES = [f"https://{CDF_CLUSTER}.cognitedata.com/.default"]
TOKEN_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
BASE_URL = f"https://{CDF_CLUSTER}.cognitedata.com"

creds = OAuthClientCredentials(token_url=TOKEN_URL, client_id=CLIENT_ID, scopes=SCOPES, client_secret=CLIENT_SECRET)
cnf = ClientConfig(client_name=COGNITE_PROJECT, project=COGNITE_PROJECT, credentials=creds, base_url=BASE_URL)
client_tsp = CogniteClient(cnf)
