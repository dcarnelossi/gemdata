from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import time
import requests
from modules.dbpgconn import WriteJsonToPostgres


# Variáveis globais
api_conection_info = None
data_conection_info = None
coorp_conection_info = None
start_date_info = None 


LIMIT = 50  # limite máximo por página

def make_request_lojaintegrada_list(offset):
    try:

        url = f'https://api.awsli.com.br/v1/pedido/search/?since_criado={start_date_info}'
        
        
    
        
        params = {
            'format': 'json',
            'chave_api': api_conection_info["apptoken"],
            'chave_aplicacao': api_conection_info["appapplication"],
            'limit': LIMIT,
            'offset': offset
        }

        response = requests.get(url, params=params)
        return response.json() if response.status_code == 200 else None

    except requests.RequestException as e:
        logging.error(f"Request failed: {e}")
        raise


def get_all_paginated_data():
    try:
            all_ids = []
            offset = 0

            while True:
                data = make_request_lojaintegrada_list(offset)
                if not data:
                    break

                objects = data.get("objects", [])
                if not objects:
                    break

                # adiciona apenas os IDs na lista
                all_ids.extend([obj["id"] for obj in objects if "id" in obj])

                meta = data.get("meta", {})
                total_count = meta.get("total_count", 0)

                offset += LIMIT
                if offset >= total_count:
                    break

            return all_ids
    except Exception as e:
            logging.error(f"Erro ao buscar dados da API Loja Integrada: {e}")
            raise




def make_request_lojaintegrada(idorders):
    try:
        

        url = f'https://api.awsli.com.br/v1/pedido/{idorders}'
        logging.info(url)
        params = {
            'format': 'json',
            'chave_api': api_conection_info["apptoken"],
            'chave_aplicacao': api_conection_info["appapplication"],
            # 'limit': LIMIT,
            # 'offset': offset
        }

        response = requests.get(url, params=params)
        return response.json() if response.status_code == 200 else None

    except requests.RequestException as e:
        logging.error(f"Request failed: {e}")
        raise


def get_api_data(idorders):
    try:
        return make_request_lojaintegrada(idorders)
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
                #result = WriteJsonToPostgres(coorp_conection_info, query_default, "integrations_parameter_api")
                result = WriteJsonToPostgres(coorp_conection_info, query_default, "integrations_parameter_api")
                
                result, _ = result.query()
            data = result[0][0]
            if query_type not in data["lojaintegrada"]:
                raise ValueError(f"Query '{query_type}' não encontrada no JSON")
            return data["lojaintegrada"][query_type]
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

    # Se data_path for vazio, considera o próprio data como lista (caso do detalhado)
    nodes = data if data_path == "" else safe_get(data, data_path, [])

    # Se for apenas um dicionário, encapsula como lista
    if isinstance(nodes, dict):
        nodes = [nodes]

    for item in nodes:
        transformed_item = {}
        context = {"item": item, "safe_get": safe_get}
        for key, path in structure.items():
            try:
                transformed_item[key] = eval(path, context) if item else None
            except Exception as e:
                logging.warning(f"Erro ao transformar campo '{key}': {e}")
                transformed_item[key] = None
        transformed_data["list"].append(transformed_item)

    return transformed_data


def process_data_batch(data_list, table, keytable):
    for attempt in range(1, 6):
        try:

            schema = "5e164a4b-5e09-4f43-9d81-a3d22b09a01b"
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
            time.sleep(40)
            if attempt == 5:
                logging.error("Falha após 5 tentativas ao salvar dados no banco.")
                raise e

def fetch_and_process(query_type,fullids):
    try:
        #json_type_api = load_graphql_query(query_type)
        
        ids = fullids
        json_type_api = load_graphql_query(query_type)
        #  endpoint = json_type_api.get("endpoint", "/api/v1/produto")
        structure = json_type_api["structure"]
        data_path = json_type_api["data_path"]
        table = json_type_api["tablepg"]
        keytable = json_type_api["keytablepg"]


        all_data = []

        for idorders in ids:
            data = get_api_data(idorders)
           
            
            if data:
                all_data.append(data)
            
            time.sleep(0.35)

            # Opcional: salvamento em lotes
            if len(all_data) >= 50:
                parsed = transform_api_response(all_data, structure, data_path)
                process_data_batch(parsed["list"],table,keytable)
                all_data = []
                               
                # Evitar sobrecarga na API
               

        if all_data:
            #print(all_data)
            parsed = transform_api_response(all_data, structure, data_path)
           
            process_data_batch(parsed["list"],table,keytable)
            

    except Exception as e:
        logging.error(f"Erro no processo de fetch/process: {e}")
        raise




def set_globals(api_info, data_conection, coorp_conection,type_api,start_date):
    global api_conection_info
    api_conection_info = api_info

    global data_conection_info
    data_conection_info = data_conection

    global coorp_conection_info
    coorp_conection_info = coorp_conection
    
 
    
    global start_date_info 
    start_date_info = start_date

    fullids=get_all_paginated_data()
    
    fetch_and_process(type_api,fullids)


