import concurrent.futures
import logging
import requests
import time
from threading import Lock
from modules.dbpgconn import WriteJsonToPostgres

# Globals
api_conection_info = None
data_conection_info = None

session = requests.Session()

# Buffer thread-safe para batches
buffer = []
buffer_lock = Lock()
BATCH_SIZE = 1000


#----------------------------------------------------------
# REQUEST COM RETRY
#----------------------------------------------------------
def make_request(method, path, params=None):
    if not api_conection_info:
        raise ValueError("API connection info is not set.")

    tentativa = 1
    max_tentativas = 3

    while tentativa <= max_tentativas:
        try:
            url = f"https://{api_conection_info['Domain']}/api/catalog_system/pvt/sku/{path}"
            #logging.info(f"Tentativa {tentativa}: Request {url}")

            response = session.request(
                method,
                url,
                params=params,
                headers=api_conection_info["headers"],
            )

            response.raise_for_status()
            return response.json() if response.status_code == 200 else None

        except Exception as e:
            logging.error(f"Tentativa {tentativa} falhou: {e}")

        tentativa += 1
        time.sleep(60)

    raise RuntimeError("Falha ao realizar requisição após múltiplas tentativas.")


#----------------------------------------------------------
# BUSCA LISTA DE SKUS
#----------------------------------------------------------
def get_skus_list_pages(page):
    params = {"page": page, "pagesize": 1000}
    return make_request("GET", "stockkeepingunitids", params=params)


def get_skus_ids(init_page):
    try:
        skus = []
        while True:
            page_data = get_skus_list_pages(init_page)
            if not page_data:
                break
            skus.extend(page_data)
            init_page += 1
        return skus
    except Exception as e:
        logging.error(f"Erro ao buscar lista SKUs: {e}")
        raise


#----------------------------------------------------------
# BUSCA DADOS INDIVIDUAIS
#----------------------------------------------------------
def get_sku_by_id(sku_id):
    return make_request("GET", f"stockkeepingunitbyid/{sku_id}")


def process_sku(sku_id):
    """Busca o SKU e adiciona no buffer."""
    try:
        data = get_sku_by_id(sku_id)
        if data:
            with buffer_lock:
                buffer.append(data)
        else:
            logging.error(f"SKU {sku_id} não encontrado")
    except Exception as e:
        logging.error(f"Erro em sku {sku_id}: {e}")


#----------------------------------------------------------
# SALVAMENTO EM BATCH REAL
#----------------------------------------------------------
def save_batch_if_needed(force=False):
    global buffer

    with buffer_lock:
        if len(buffer) < BATCH_SIZE and not force:
            return

        # Monta o batch
        if force:
            batch = buffer[:]      
            buffer.clear()
        else:
            batch = buffer[:BATCH_SIZE]
            del buffer[:BATCH_SIZE]

    if not batch:
        return

    try:
        logging.info(f"Gravando batch de {len(batch)} itens no Postgres...")

        writer = WriteJsonToPostgres(
            data_conection_info,
            batch,
            "skus",
            "Id"
        )
        writer.upsert_data_batch_otimizado()

        logging.info("Batch salvo com sucesso.")

    except Exception as e:
        logging.error(f"Erro ao salvar batch: {e}")
        raise


#----------------------------------------------------------
# PROCESSAMENTO PRINCIPAL — PRODUCER/CONSUMER REAL
#----------------------------------------------------------
def get_skus(init_page):
    if not api_conection_info or not data_conection_info:
        raise ValueError("Connection info not set.")

    try:
        skus = get_skus_ids(init_page)
        if not skus:
            logging.info("Nenhum SKU encontrado.")
            return

        logging.info(f"Total SKUs encontrados: {len(skus)}")

        # Thread pool
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:

            futures = []

            for sku in skus:
                future = executor.submit(process_sku, sku)
                futures.append(future)

                # Salva o batch assim que a thread termina
                future.add_done_callback(lambda f: save_batch_if_needed())

                # PROTEGE A API DA VTEX
                time.sleep(0.05)  # 50ms entre submissões (evita estouro da API)

            # Aguarda todas finalizarem
            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Erro em thread: {e}")

        # Salva qualquer sobra do buffer
        save_batch_if_needed(force=True)

    except Exception as e:
        logging.error(f"Erro geral: {e}")
        raise


#----------------------------------------------------------
# SET GLOBALS
#----------------------------------------------------------
def set_globals(init_page, api_info, conection_info):
    global api_conection_info, data_conection_info

    api_conection_info = api_info
    data_conection_info = conection_info

    get_skus(init_page)
