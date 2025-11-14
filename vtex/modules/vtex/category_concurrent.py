import concurrent.futures
import json
import logging
import time
from threading import Lock
import requests
from modules.dbpgconn import WriteJsonToPostgres

#---------------------------------------
# GLOBALS
#---------------------------------------
api_conection_info = None
data_conection_info = None
category_levels = None

# buffer de batches
buffer = []
buffer_lock = Lock()
BATCH_SIZE = 1000  # categorias podem ser muitas → 1000 é bom


#---------------------------------------
# REQUEST PADRÃO (com retry implícito)
#---------------------------------------
def make_request(method, endpoint, params=None):
    url = f"https://{api_conection_info['Domain']}{endpoint}"

    try:
        response = requests.request(
            method,
            url,
            headers=api_conection_info["headers"],
            params=params,
            timeout=30
        )
        response.raise_for_status()
        return response.json()

    except Exception as e:
        logging.error(f"Erro na requisição para {url}: {e}")
        raise


#---------------------------------------
# SALVA BATCH REAL
#---------------------------------------
def save_batch_if_needed(force=False):
    global buffer

    with buffer_lock:
        if len(buffer) < BATCH_SIZE and not force:
            return

        # cria o batch
        if force:
            batch = buffer[:]
            buffer.clear()
        else:
            batch = buffer[:BATCH_SIZE]
            del buffer[:BATCH_SIZE]

    if not batch:
        return

    try:
        logging.info(f"Gravando batch de {len(batch)} categorias...")

        writer = WriteJsonToPostgres(
            data_conection_info,
            batch,
            "categories",
            "Id"
        )

        writer.upsert_data_batch_otimizado()

        logging.info("Batch de categorias salvo com sucesso.")

    except Exception as e:
        logging.error(f"Erro ao gravar batch de categorias: {e}")
        raise


#---------------------------------------
# PROCESSAR UMA CATEGORIA INDIVIDUAL
#---------------------------------------
def process_category_id(category_id):
    try:
        category_details = make_request(
            "GET",
            f"/api/catalog/pvt/category/{category_id}"
        )

        if not category_details:
            logging.error(f"Categoria {category_id} retornou vazio.")
            return

        # adiciona ao buffer
        with buffer_lock:
            buffer.append(category_details)

    except Exception as e:
        logging.error(f"Erro processando categoria {category_id}: {e}")


#---------------------------------------
# EXTRAI IDS (recursivo)
#---------------------------------------
def extract_category_ids(raw_json):
    def extrair(obj):
        ids = [obj["id"]]
        if "children" in obj:
            for child in obj["children"]:
                ids.extend(extrair(child))
        return ids

    try:
        data = json.loads(raw_json)
        ids_total = []
        for item in data:
            ids_total.extend(extrair(item))
        return ids_total

    except Exception as e:
        logging.error(f"Erro extraindo IDs de categorias: {e}")
        raise


#---------------------------------------
# PROCESSAR TODAS AS CATEGORIAS
# (Producer–Consumer)
#---------------------------------------
def process_categories_tree(category_levels):

    #---------------------------------------
    # 1) BUSCA ARVORE DE CATEGORIAS
    #---------------------------------------
    logging.info("Buscando árvore de categorias...")

    data = make_request(
        "GET",
        f"/api/catalog_system/pub/category/tree/{category_levels}"
    )

    if not data:
        logging.error("Falha ao obter árvore de categorias")
        return

    # transforma JSON -> lista de IDs
    raw = json.dumps(data)
    category_ids = extract_category_ids(raw)

    logging.info(f"Total de categorias encontradas: {len(category_ids)}")

    #---------------------------------------
    # 2) PROCESSA CATEGORIAS EM PARALELO
    #---------------------------------------
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:

        futures = []

        for cat_id in category_ids:

            future = executor.submit(process_category_id, cat_id)
            futures.append(future)

            # salva batch assim que thread termina
            future.add_done_callback(lambda f: save_batch_if_needed())

            # suaviza API VTEX
            time.sleep(0.05)

        # aguarda todas terminarem
        for future in futures:
            try:
                future.result()
            except Exception as e:
                logging.error(f"Erro em thread de categoria: {e}")

    #---------------------------------------
    # 3) SALVA O RESTANTE
    #---------------------------------------
    save_batch_if_needed(force=True)

    logging.info("Processamento de categorias finalizado.")


#---------------------------------------
# SET GLOBALS
#---------------------------------------
def set_globals(categories_info, api_info, conection_info):
    global category_levels
    category_levels = categories_info

    global api_conection_info
    api_conection_info = api_info

    global data_conection_info
    data_conection_info = conection_info

    process_categories_tree(categories_info)
