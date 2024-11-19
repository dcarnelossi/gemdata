import logging
import time
from datetime import datetime
from modules.dbpgconn import WriteJsonToPostgres

def log_process(coorp_conection_info,value):
    try: 
          
                       
            writer = WriteJsonToPostgres(coorp_conection_info , value , "log_import_import", "dag_run_id,id")
            writer.upsert_data()
            logging.info(f"upserted do log diario successfully.")
    except Exception as e:
            logging.error(f"Error inserting log diario: {e}")
            raise e  # Ensure failure is propagated to Airflow

    
# report_id = str(uuid.uuid4()) 

# dataatual =   datetime.now()

# data = {
#     'id':report_id ,
#     'integration_id': report_id,
#     'nameprocess': "daily",
#     'dag': "teste",
#     'dag_run_id': "aaav",
#     'dag_started_at': dataatual,
     
# }

# log_process("appgemdata-pgserver-prod",data)
