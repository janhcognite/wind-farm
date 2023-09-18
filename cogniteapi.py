import os

from cognite.client import CogniteClient, ClientConfig
from cognite.client.credentials import OAuthClientCredentials

CDF_CLUSTER = "api"
TENANT_ID = os.getenv("TENANT_ID_TECHBYCOGNITE")
CLIENT_ID = os.getenv("CLIENT_ID_TECHBYCOGNITE_PYTHON_SDK")
CLIENT_SECRET = os.getenv("CLIENT_SECRET_TECHBYCOGNITE_PYTHON_SDK")

COGNITE_PROJECT = "renewable-tech-staging"

SCOPES = [f"https://{CDF_CLUSTER}.cognitedata.com/.default"]
TOKEN_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
BASE_URL = f"https://{CDF_CLUSTER}.cognitedata.com"

creds = OAuthClientCredentials(token_url=TOKEN_URL, client_id= CLIENT_ID, scopes= SCOPES, client_secret= CLIENT_SECRET)
cnf = ClientConfig(client_name="renewable-tech-staging", project=COGNITE_PROJECT, credentials=creds, base_url=BASE_URL)
client = CogniteClient(cnf)


