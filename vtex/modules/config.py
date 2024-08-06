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
# from azure.storage.blob import BlobServiceClient
# from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError
# from azure.cosmos import CosmosClient, exceptions, PartitionKey


from vtex.modules.helpers import *


logger = logging.getLogger(__name__)


load_dotenv()


PGHOST=None
PGUSER=None
PGPORT=5432
PGDATABASE=None
PGPASSWORD=None
PGSCHEMA=None




#
# Configure logging
#
#logging.basicConfig(filename='error_log.txt', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
