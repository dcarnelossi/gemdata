import os
import math
import time
import logging
from dotenv import load_dotenv
from datetime import datetime, timedelta
import http.client
import requests
from urllib.parse import urlencode
import json
import concurrent.futures
from collections import OrderedDict
import psycopg2
from psycopg2.extras import Json

# import openai
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError
#from azure.cosmos import CosmosClient, exceptions, PartitionKey


from vtex.modules.helpers import *


logger = logging.getLogger(__name__)


load_dotenv()

    
# DB Dados configuratiuon

# PGHOST="pggemdata-dev-00.postgres.database.azure.com"
# PGUSER="adminuserpggemdatadev"
# PGPORT=5432
# PGDATABASE="db-vetex-dev-00"
# PGPASSWORD="qgfC64psgk7CCveWRFHAPCQR0F3DPzdxIUW7uD2HaHxuG0MoemnvpHYlCeM5"
# PGSCHEMA="mahogany"

PGHOST=None
PGUSER=None
PGPORT=5432
PGDATABASE=None
PGPASSWORD=None
PGSCHEMA=None


# PGHOST=Variable.get("PGHOST")
# PGUSER=Variable.get("PGUSER")
# PGPORT=5432
# PGDATABASE=Variable.get("PGDATABASE")
# PGPASSWORD=Variable.get("PGPASSWORD")
# PGSCHEMA = None

# VETEX configuratiuon

# VTEX_API_AppKey = os.getenv("VTEX_API_AppKey")
# VTEX_API_AppToken = os.getenv("VTEX_API_AppToken")
# VTEX_API_AccountName = os.getenv("VTEX_API_AccountName")
# VTEX_API_Environment = os.getenv("VTEX_API_Environment")
# VTEX_Domain = f"{VTEX_API_AccountName}.{VTEX_API_Environment}.com.br"



# VTEX_API_AppKey="vtexappkey-mahogany-BLBUCK"
# VTEX_API_AppToken="FPBMGFGTPYJAYLFAZOUAOBPKCXKUFZOHQMCJEMWDBXYCRSNQWNUBISKFEKLHYGWJXJGGAFVFHSMDVLBHIOSUIQMEYFCLDFYYWUZXNYNBCNANLOYTRZPFFMYMCHYZYDOF"
# VTEX_API_AccountName="mahogany"
# VTEX_API_Environment="vtexcommercestable"
# VTEX_Domain = f"{VTEX_API_AccountName}.{VTEX_API_Environment}.com.br"

# headers = {
#     'Accept': "application/json",
#     'Content-Type': "application/json",
#     'X-VTEX-API-AppKey': VTEX_API_AppKey,
#     'X-VTEX-API-AppToken': VTEX_API_AppToken
#     }





# # OPENAI configuratiuon

# OPENAI_APIKEY="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"
# OPENAI_ENGINE="gpt-4"

# # AZURE BLOB configuration

# account_key = "+XTzmkyTW01sinpntqV5FcSM5//kLil3TTPDcuAyNEoKn/mLVaKui5S1M5Yu8pfedjvuof5ZGW0h+AStgv1uNA=="
# account_name = "vetexstorageraw"
# container_name = "vetexraw"
# #file_name = "categories.json"


# # # AZURE Cosmos configuration
# # endpoint = "https://cosmodbnosql.documents.azure.com:443/"
# # key = "SL0SzTsPzO0XYot5BKFza0KzufP3UP8K1WTBnqAhKAnEK56e2xGHp1m36QlDdpyPurJEFohwBMBIACDbtK5s6g=="
# # database_id = VTEX_API_AccountName




#
# Configure logging
#
#logging.basicConfig(filename='error_log.txt', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')