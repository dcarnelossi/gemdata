import concurrent.futures
import http.client
import json
import logging
import time
from threading import Lock
from dbpgconn import WriteJsonToPostgres

#---------------------------------------
# GLOBALS
#---------------------------------------
api_conection_info = None
data_conection_info = None

buffer = []
buffer_lock = Lock()
BATCH_SIZE = 500   # marcas são poucas → 500 é excelente


#---------------------------------------
# REQUEST DE MARCA (VTEX)
#---------------------------------------
def request_get(path):
    try:
        conn = http.client.HTTPSConnection(api_conection_info["Domain"])
        conn.request("GET", path, headers=api_conection_info["headers"])
        res = conn.getresponse()
        data = res.read()
        conn.close()

        if res.status == 200:
            return json.loads(data.decode("utf-8"))
        else:
            logging.error(f"Erro VTEX GET {path} → {res.status} - {res.reason}")
            return None

    except Exception as e:
        logging.error(f"Request GET erro em {path}: {e}")
        return None


#---------------------------------------
# SALVA EM BATCH
#---------------------------------------
def save_batch_if_needed(force=False):
    global buffer

    with buffer_lock:
        if len(buffer) < BATCH_SIZE and not force:
            return

        if force:
            batch = buffer[:]
            buffer.clear()
        else:
            batch = buffer[:BATCH_SIZE]
            del buffer[:BATCH_SIZE]

    if not batch:
        return

    try:
        logging.info(f"Gravando batch de {len(batch)} brands...")

        writer = WriteJsonToPostgres(
            data_conection_info,
            batch,
            "brands",
            "Id"
        )

        writer.upsert_data_batch_otimizado()

        logging.info("Batch de brands salvo com sucesso.")

    except Exception as e:
        logging.error(f"Erro ao salvar batch brands: {e}")
        raise


#---------------------------------------
# PROCESSA UMA MARCA INDIVIDUAL
#---------------------------------------
def process_brand(brand_id):
    """Busca dados da brand e adiciona ao buffer."""
    try:
        data = request_get(f"/api/catalog/pvt/brand/{brand_id}")
        if not data:
            logging.error(f"Brand {brand_id} retornou vazio.")
            return

        with buffer_lock:
            buffer.append(data)

    except Exception as e:
        logging.error(f"process_brand {brand_id} → erro: {e}")


#---------------------------------------
# EXTRAI TODOS OS BRAND IDs (recursivo)
#---------------------------------------
def extract_brand_ids(raw_json):
    try:
        dados_json = json.loads(raw_json)
        ids = []

        def extrair(obj):
            ids.append(obj["id"])
            if "children" in obj:
                for c in obj["children"]:
                    extrair(c)

        for root in dados_json:
            extrair(root)

        return ids

    except Exception as e:
        logging.error(f"Erro extraindo brand IDs: {e}")
        raise


#---------------------------------------
# PROCESSAMENTO PRINCIPAL (Producer–Consumer)
#---------------------------------------
def get_brands_parallel():
    logging.info("Iniciando processamento de brands...")

    #---------------------------------
    # 1) GET LISTA DE TODAS AS BRANDS
    #---------------------------------
    conn = http.client.HTTPSConnection(api_conection_info["Domain"])
    conn.request("GET", "/api/catalog_system/pvt/brand/list", headers=api_conection_info["headers"])
    res = conn.getresponse()
    data = res.read().decode("utf-8")
    conn.close()

    if res.status != 200:
        logging.error(f"Erro ao buscar lista de brands: {res.status}")
        raise RuntimeError("Falha no GET /brand/list")

    brand_ids = extract_brand_ids(data)
    logging.info(f"Total de brands encontradas: {len(brand_ids)}")

    #---------------------------------
    # 2) PROCESSA EM PARALELO + BATCH
    #---------------------------------
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:

        futures = []

        for brand_id in brand_ids:
            future = executor.submit(process_brand, brand_id)
            futures.append(future)

            # executa salvamento incremental sempre que uma thread termina
            future.add_done_callback(lambda f: save_batch_if_needed())

            # suaviza carga VTEX
            time.sleep(0.05)

        # Espera todas finalizarem
        for future in futures:
            try:
                future.result()
            except Exception as e:
                logging.error(f"Erro em thread de brand: {e}")

    #---------------------------------
    # 3) SALVA RESTO DO BUFFER
    #---------------------------------
    save_batch_if_needed(force=True)

    logging.info("Processamento de brands finalizado.")


#---------------------------------------
# SET GLOBALS
#---------------------------------------
def set_globals(api_info, conection_info):
    global api_conection_info, data_conection_info
    api_conection_info = api_info
    data_conection_info = conection_info

    get_brands_parallel()
