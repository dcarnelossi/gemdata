from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import time
import requests
from modules.dbpgconn import WriteJsonToPostgres

from modules.helpers import increment_one_day

from modules.api_conection import make_request_token_moovin,make_request_api_moovin

# Variáveis globais
api_conection_info = None
data_conection_info = None
coorp_conection_info = None
# isdaily_info = None
# start_date_info = None 
token_info = None


LIMIT = 50  # limite máximo por página

def get_access_token():
    try:
        return make_request_token_moovin(api_conection_info["apikey"],
                                         api_conection_info["apisecret"],
                                         api_conection_info["accountclientid"])
    
    except requests.RequestException as e:
        logging.error(f"Request failed: {e}")
        raise


def make_request_moovin(offset):
    try:
       
        # if isdaily_info:
        #     url = f'https://api.moovin.store/oms-product/category?updatedAt={start_date_info}'
        # else:
        url = f'https://api.moovin.store/oms-product/category?size={LIMIT}&offset={offset}'
        

        return make_request_api_moovin(token_info,url,50)

    except requests.RequestException as e:
        logging.error(f"Request failed: {e}")
        raise



def get_all_paginated_data():
    all_data = []
    offset = 0

    while True:

        data = make_request_moovin(offset)
   
        if not data:
            break

        objects = data.get("items", [])
        if not objects:
            break

        all_data.extend(objects)
        
        total_count = data.get("total", 0)

        offset += LIMIT
        if offset >= total_count:
            break

    return {"items": all_data}


def get_api_data():
    try:
        return get_all_paginated_data()
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
            if query_type not in data["moovin"]:
                raise ValueError(f"Query '{query_type}' não encontrada no JSON")
            return data["moovin"][query_type]
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
        context = {"item": item, "safe_get": safe_get}
        for key, path in structure.items():
            try:
                transformed_item[key] = eval(path, context) if item else None
            except Exception:
                transformed_item[key] = None
        transformed_data["list"].append(transformed_item)
    return transformed_data



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

# >>> ADICIONE PERTO DOS IMPORTS/HELPERS
def dedup_por_chave(rows, key_name: str, keep: str = "last"):
    """
    Remove duplicatas da lista de dicts com base em key_name.
    keep='last' mantém a última ocorrência; keep='first' mantém a primeira.
    """
    if keep not in ("last", "first"):
        keep = "last"

    if keep == "last":
        # última ocorrência vence
        mapa = {r.get(key_name): r for r in rows if r.get(key_name) is not None}
        unicos = list(mapa.values())
    else:
        vistos = set()
        unicos = []
        for r in rows:
            k = r.get(key_name)
            if k is None:
                continue
            if k not in vistos:
                vistos.add(k)
                unicos.append(r)

    # opcional: manter ordem estável por conveniência (não obrigatório)
    # ordena por primeira aparição do id original (quando keep='last')
    if keep == "last":
        ordem = {}
        for i, r in enumerate(rows):
            k = r.get(key_name)
            if k is not None and k not in ordem:
                ordem[k] = i
        unicos.sort(key=lambda r: ordem.get(r.get(key_name), 10**12))

    return unicos

def fetch_and_process(query_type):
    try:
        json_type_api = load_graphql_query(query_type)

        structure = json_type_api["structure"]
        data_path = json_type_api["data_path"]
        table = json_type_api["tablepg"]
        keytable = json_type_api["keytablepg"]  # ex.: "id"

        response = get_api_data()
        parsed = transform_api_response(response, structure, data_path)

        # >>> DEDUP AQUI <<<
        lista_original = parsed["list"]
        lista_unica = dedup_por_chave(lista_original, keytable, keep="last")

        # Logs úteis
        removidos = len(lista_original) - len(lista_unica)
        if removidos > 0:
            # coleta alguns ids repetidos para log (sem poluir)
            ids_orig = [r.get(keytable) for r in lista_original if r.get(keytable) is not None]
            repetidos = []
            vistos = set()
            for _id in ids_orig:
                if _id in vistos and _id not in repetidos:
                    repetidos.append(_id)
                else:
                    vistos.add(_id)
            logging.info(f"[dedup] Removidos {removidos} duplicados por '{keytable}'. Exemplos: {repetidos[:5]}")

        process_data_batch(lista_unica, table, keytable)

    except Exception as e:
        logging.error(f"Erro no processo de fetch/process: {e}")
        raise

def set_globals(api_info, data_conection, coorp_conection,type_api):
    global api_conection_info
    api_conection_info = api_info

    global data_conection_info
    data_conection_info = data_conection

    global coorp_conection_info
    coorp_conection_info = coorp_conection

    # global  isdaily_info 
    # isdaily_info = isdaily
    
    # global start_date_info 
    # start_date_info = start_date
    
    global token_info 
  
    token_info=get_access_token()

    fetch_and_process(type_api)


# set_globals("a","integrations-data-dev","appgemdata-homol","products")
