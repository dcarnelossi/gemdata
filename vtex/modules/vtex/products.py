import concurrent.futures
import logging
import time
from threading import Lock

from modules.api_conection import make_request
from modules.dbpgconn import WriteJsonToPostgres


#----------------------------------------------------
# GLOBALS
#----------------------------------------------------
api_conection_info = None
data_conection_info = None

buffer = []
buffer_lock = Lock()
BATCH_SIZE = 500   # products são pesados → 500 é ideal


#----------------------------------------------------
# BUSCA IDS NO BANCO
#----------------------------------------------------
def get_products_ids_from_db():
    try:
        query = """
            SELECT DISTINCT CAST(productid AS INT) AS id 
            FROM skus;
        """
        result = WriteJsonToPostgres(data_conection_info, query, "skus").query()

        if not result:
            logging.warning("Nenhum productId encontrado no banco.")

        return [row[0] for row in result[0]] if result else []

    except Exception as e:
        logging.error(f"Erro em get_products_ids_from_db: {e}")
        raise


#----------------------------------------------------
# BUSCA 1 PRODUCT DA API
#----------------------------------------------------
def get_product_by_id(product_id):
    return make_request(
        api_conection_info["Domain"],
        "GET",
        f"api/catalog/pvt/product/{product_id}",
        headers=api_conection_info["headers"],
    )


#----------------------------------------------------
# SALVA BATCH
#----------------------------------------------------
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
        logging.info(f"Gravando batch de {len(batch)} products...")

        writer = WriteJsonToPostgres(
            data_conection_info,
            batch,
            "products",
            "Id"
        )

        writer.upsert_data_batch_otimizado()

        logging.info("Batch de products salvo com sucesso.")

    except Exception as e:
        logging.error(f"Erro ao salvar batch de products: {e}")
        raise


#----------------------------------------------------
# PROCESSA UM PRODUCT INDIVIDUAL
#----------------------------------------------------
def process_product(product_id):
    try:
        data = get_product_by_id(product_id)

        if not data:
            logging.error(f"Product {product_id} retornou vazio.")
            return

        with buffer_lock:
            buffer.append(data)

    except Exception as e:
        logging.error(f"Erro processando product_id {product_id}: {e}")


#----------------------------------------------------
# PROCESSA TODOS OS PRODUCTS (Producer–Consumer)
#----------------------------------------------------
def process_products():
    try:
        product_ids = get_products_ids_from_db()

        if not product_ids:
            logging.warning("Nenhum productId disponível para processamento.")
            return

        logging.info(f"Total de products a processar: {len(product_ids)}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:

            futures = []

            for pid in product_ids:

                future = executor.submit(process_product, pid)
                futures.append(future)

                # Salva batch assim que uma thread termina
                future.add_done_callback(lambda f: save_batch_if_needed())

                # Suaviza carga na VTEX
                time.sleep(0.05)

            # Espera todas terminarem
            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Erro numa thread de produto: {e}")

        # Salva resto
        save_batch_if_needed(force=True)

        logging.info("Processamento de products finalizado.")

    except Exception as e:
        logging.error(f"Erro geral em process_products: {e}")
        raise


#----------------------------------------------------
# SET GLOBALS E EXECUTA
#----------------------------------------------------
def set_globals(api_info, conection_info):
    global api_conection_info, data_conection_info

    api_conection_info = api_info
    data_conection_info = conection_info

    if not api_conection_info or not data_conection_info:
        raise ValueError("Informações globais de conexão não definidas.")

    process_products()
