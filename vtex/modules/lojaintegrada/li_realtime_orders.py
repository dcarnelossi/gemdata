from datetime import datetime, timedelta
import logging
import time
import requests
from modules.dbpgconn import WriteJsonToPostgres

# Variáveis globais (mantidas, mas ideal seria injetar por parâmetro)
api_conection_info = None
data_conection_info = None
coorp_conection_info = None
start_date_info = None

LIMIT = 50
HTTP_TIMEOUT = 30

# ----------- HTTP -----------
def request_pedidos_paginado(offset: int):
    try:
        url = "https://api.awsli.com.br/v1/pedido/search/"
        params = {
            "format": "json",
            "chave_api": api_conection_info["apptoken"],
            "chave_aplicacao": api_conection_info["appapplication"],
            "limit": LIMIT,
            "offset": offset,
            "since_criado": start_date_info,
        }
        resp = requests.get(url, params=params, timeout=HTTP_TIMEOUT)
        if resp.status_code != 200:
            logging.error(f"[search] {resp.status_code} - {resp.text}")
            return None
        return resp.json()
    except requests.RequestException as e:
        logging.error(f"[search] Request failed: {e}")
        raise

def request_pedido_por_id(idpedido: int | str):
    try:
        # confirme se precisa de barra final:
        url = f"https://api.awsli.com.br/v1/pedido/{idpedido}"
        params = {
            "format": "json",
            "chave_api": api_conection_info["apptoken"],
            "chave_aplicacao": api_conection_info["appapplication"],
        }
        resp = requests.get(url, params=params, timeout=HTTP_TIMEOUT)
        if resp.status_code != 200:
            logging.error(f"[detail:{idpedido}] {resp.status_code} - {resp.text}")
            return None
        return resp.json()
    except requests.RequestException as e:
        logging.error(f"[detail:{idpedido}] Request failed: {e}")
        raise

# ----------- paginação -----------
def get_all_paginated_data():
    try:
        all_ids = []
        offset = 0
        while True:
            data = request_pedidos_paginado(offset)
            if not data:
                break
            objects = data.get("objects", [])
            if not objects:
                break
            all_ids.extend([obj["id"] for obj in objects if "id" in obj])

            meta = data.get("meta", {})
            total_count = meta.get("total_count", 0)
            offset += LIMIT
            if offset >= total_count:
                break
        return all_ids
    except Exception as e:
        logging.error(f"Erro ao paginar Loja Integrada: {e}")
        raise

# ----------- config dinâmica -----------
def load_graphql_query(query_type):
    for attempt in range(1, 6):
        try:
            query_modificado = f"""
                SELECT parameter FROM public.integrations_parameter_api
                WHERE id = '{data_conection_info['schema']}'
                ORDER BY date_modification DESC
                LIMIT 1;
            """
            result = WriteJsonToPostgres(coorp_conection_info, query_modificado, "integrations_parameter_api")
            result, _ = result.query()

            if not result or not result[0]:
                query_default = """
                    SELECT parameter FROM public.integrations_parameter_api
                    WHERE name = 'default'
                    ORDER BY date_modification DESC
                    LIMIT 1;
                """
                result = WriteJsonToPostgres(coorp_conection_info, query_default, "integrations_parameter_api")
                result, _ = result.query()

            data = result[0][0]
            if query_type not in data["lojaintegrada"]:
                raise ValueError(f"Query '{query_type}' não encontrada no JSON")

            return data["lojaintegrada"][query_type]
        except Exception as e:
            logging.warning(f"[Tentativa {attempt}/5] Erro ao buscar tipo '{query_type}': {e}")
            time.sleep(2)
            if attempt == 5:
                logging.error("Falha após 5 tentativas ao buscar definição no banco.")
                raise

# ----------- utils -----------
def safe_get(dictionary, path, default=None):
    keys = path.split(".")
    value = dictionary
    try:
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        # se quiser considerar {} como valor válido, remova '{}' daqui
        return value if value not in ["", None, {}] else default
    except (KeyError, TypeError, AttributeError):
        return default

def transform_api_response(data, structure, data_path: str):
    transformed_data = {"list": []}
    nodes = data if data_path == "" else safe_get(data, data_path, [])

    if isinstance(nodes, dict):
        nodes = [nodes]

    for item in nodes:
        transformed_item = {}
        context = {"item": item, "safe_get": safe_get}
        for key, expr in structure.items():
            try:
                # usa locals, sem poluir globals
                transformed_item[key] = eval(expr, {}, context) if item else None
            except Exception as e:
                logging.warning(f"Erro ao transformar campo '{key}': {e}")
                transformed_item[key] = None
        transformed_data["list"].append(transformed_item)
    return transformed_data

# ----------- persistência -----------
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
            return
        except Exception as e:
            logging.warning(f"[Tentativa {attempt}/5] Erro ao salvar dados: {e}")
            time.sleep(40)
            if attempt == 5:
                logging.error("Falha após 5 tentativas ao salvar dados no banco.")
                raise

# ----------- fluxo principal -----------
def fetch_and_process(query_type, ids):
    try:
        json_type_api = load_graphql_query(query_type)
        structure = json_type_api["structure"]
        data_path = json_type_api["data_path"]
        table = json_type_api["tablepg"]
        keytable = json_type_api["keytablepg"]

        batch = []
        for idpedido in ids:
            data = request_pedido_por_id(idpedido)
            if data:
                batch.append(data)

            # backoff leve para evitar rate limit
            time.sleep(0.35)

            if len(batch) >= 50:
                parsed = transform_api_response(batch, structure, data_path)
                process_data_batch(parsed["list"], table, keytable)
                batch = []

        if batch:
            parsed = transform_api_response(batch, structure, data_path)
            process_data_batch(parsed["list"], table, keytable)

    except Exception as e:
        logging.error(f"Erro no fetch/process: {e}")
        raise

def set_globals(api_info, data_conection, coorp_conection, type_api, start_date):
    global api_conection_info, data_conection_info, coorp_conection_info, start_date_info
    api_conection_info = api_info
    data_conection_info = data_conection
    coorp_conection_info = coorp_conection
    start_date_info = start_date  # garanta ISO-8601

    ids = get_all_paginated_data()
    fetch_and_process(type_api, ids)
