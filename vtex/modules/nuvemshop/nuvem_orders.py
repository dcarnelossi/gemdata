from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import time
import requests
from modules.dbpgconn import WriteJsonToPostgres

from modules.helpers import increment_one_day

from modules.api_conection import make_request_token_nuvem

# Variáveis globais
api_conection_info = None
data_conection_info = None
coorp_conection_info = None
isdaily_info = None
start_date_info = None 
end_date_info = None 


def make_request_nuvem(start_date,end_date,page):
    try:

   
        if isdaily_info:
            path = f'orders?updated_at_min={start_date}&updated_at_max={end_date}'
        else:
            path = f'orders?created_at_min={start_date}&created_at_max={end_date}'
        
     

        return make_request_token_nuvem(api_conection_info["Domain"],path,api_conection_info["headers"],page)

    
    except requests.RequestException as e:
        logging.error(f"Request failed: {e}")
        raise



def get_all_paginated_data(start_date,end_date):
    all_data = []
    page = 1

    while True:
       
        data = make_request_nuvem(start_date,end_date,page)
       
        if not data:
            break

        all_data.extend(data)
        page += 1


    return {"items": all_data}


def get_api_data(start_date,end_date):
    try:
        return get_all_paginated_data(start_date,end_date)
    except Exception as e:
        logging.error(f"Erro ao buscar dados da API Loja Integrada: {e}")
        raise

def load_graphql_query(query_type):
    for attempt in range(1, 6):
        try:
            result =[]
            query_modificado = f"""
                SELECT parameter FROM public.integrations_parameter_api
                WHERE id=  '{data_conection_info['schema']}'
                ORDER BY date_modification DESC
                LIMIT 1;
            """
         
            result = WriteJsonToPostgres(coorp_conection_info, query_modificado, "integrations_parameter_api")
            result, _ = result.query()
            if not result or not result[0]:
                query_default = f"""
                    SELECT parameter FROM public.integrations_parameter_api
                    WHERE name=  'default'
                    ORDER BY date_modification DESC
                    LIMIT 1;
                """
                result = WriteJsonToPostgres(coorp_conection_info, query_default, "integrations_parameter_api")
                
                result, _ = result.query()
            data = result[0][0]
            if query_type not in data["nuvemshop"]:
                raise ValueError(f"Query '{query_type}' não encontrada no JSON")
            return data["nuvemshop"][query_type]
        except Exception as e:
            logging.warning(f"[Tentativa {attempt}/5] Erro ao buscar definição do tipo '{query_type}': {e}")
            time.sleep(2)
            if attempt == 5:
                logging.error("Falha após 5 tentativas ao buscar definição no banco.")
                raise e



def safe_get(dictionary, path, default=None):
    keys = path.split(".")
    value = dictionary
    try:
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        return value if value not in ["", None, {}] else default
    except (KeyError, TypeError, AttributeError):
        return default

def transform_api_response(data, structure, data_path):
    transformed_data = {"list": []}
    nodes = safe_get(data, data_path, [])
    for item in nodes:
        transformed_item = {}
        context = {"item": item, "safe_get": safe_get,  "extract_main_category_info": extract_main_category_info}
        for key, path in structure.items():
            try:
                transformed_item[key] = eval(path, context) if item else None
            except Exception:
                transformed_item[key] = None
        transformed_data["list"].append(transformed_item)
    return transformed_data
def extract_main_category_info(item):
    """
    Retorna (id, name_pt) da primeira categoria cujo parent é None.
    Se não existir, devolve (None, None).
    """
    for cat in item.get("categories", []):
        if cat.get("parent") is None:
            return (
                cat.get("id"),
                safe_get(cat.get("name", {}), "pt")
            )
    return None, None

def process_data_batch(data_list, table, keytable):
    for attempt in range(1, 6):
        try:

            writer = WriteJsonToPostgres(
                data_conection_info,
                data_list,
                table,
                keytable
            )
       
            writer.upsert_data_batch(isdatainsercao=1)

        #    writer = WriteJsonToPostgres(data_conection_info, data_list, table, keytable)
           # writer.upsert_data_batch(isdatainsercao=1)
            return
        except Exception as e:
            logging.warning(f"[Tentativa {attempt}/5] Erro ao salvar dados: {e}")
            time.sleep(10)
            if attempt == 5:
                logging.error("Falha após 5 tentativas ao salvar dados no banco.")
                raise e

def fetch_and_process(json_type_api,start_date,end_date):
    try:
        #json_type_api = load_graphql_query(query_type)
        
      
      
      #  endpoint = json_type_api.get("endpoint", "/api/v1/produto")
        structure = json_type_api["structure"]
        data_path = json_type_api["data_path"]
        table = json_type_api["tablepg"]
        keytable = json_type_api["keytablepg"]

        response = get_api_data(start_date,end_date)
        parsed = transform_api_response(response, structure, data_path)

        # print(parsed)
        process_data_batch(parsed["list"], table, keytable)

    except Exception as e:
        logging.error(f"Erro no processo de fetch/process: {e}")
        raise



def process_orders_lists(type_api):
    try:
        
        data_inicial, data_final = validate_and_convert_dates(start_date_info, end_date_info)

        json_type_api = load_graphql_query(type_api)
        
        while data_inicial <= data_final:
            start_date, end_date = increment_one_day(data_inicial)
            
            logging.info(f"Processing orders from {start_date} to {end_date}.")
            # print(f"aaaaaaaaaaaaaa{start_date}")
            # if(start_date=='2024-10-09T02:00:00.000000Z'):
            fetch_and_process(json_type_api,start_date,end_date)
            data_inicial += timedelta(days=1)

    except Exception as e:
        logging.error(f"An unexpected error occurred while processing orders: {e}")
        raise  # Fail the task in case of any error


def validate_and_convert_dates(start_date, end_date):
    try:
        if not isinstance(start_date, datetime):
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
        if not isinstance(end_date, datetime):
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
        return start_date, end_date
    except ValueError as e:
        logging.error(f"Invalid date format: {e}")
        raise  # Ensure Airflow fails if date conversion fails


def set_globals(api_info, data_conection, coorp_conection,type_api,start_date,end_date, isdaily):
    global api_conection_info
    api_conection_info = api_info

    global data_conection_info
    data_conection_info = data_conection

    global coorp_conection_info
    coorp_conection_info = coorp_conection
    
    global  isdaily_info 
    isdaily_info = isdaily
    
    global start_date_info 
    start_date_info = start_date

    global end_date_info 
    end_date_info = end_date

    if not all([api_conection_info, data_conection_info, coorp_conection_info,start_date_info,end_date_info]):
        logging.error("Global connection information is incomplete.")
        raise ValueError("All global connection information must be provided.")

    process_orders_lists(type_api)


# set_globals("a","integrations-data-dev","appgemdata-homol","products")
