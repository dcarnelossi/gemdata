import logging

from dotenv import load_dotenv

logger = logging.getLogger(__name__)


load_dotenv()


PGHOST = None
PGUSER = None
PGPORT = 5432
PGDATABASE = None
PGPASSWORD = None
PGSCHEMA = None


#
# Configure logging
#
# logging.basicConfig(filename='error_log.txt', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
