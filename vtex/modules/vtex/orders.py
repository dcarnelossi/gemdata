import concurrent.futures
import logging
import time
from threading import Lock

from modules.api_conection import make_request
from modules.dbpgconn import WriteJsonToPostgres

# ==========================================================
# GLOBALS
# ==========================================================
api_conection_info = None
data_conection_info = None
coorp_conection_info = None
gb_daily = None

buffer = []
buffer_lock = Lock()
BATCH_SIZE = 300     # OMS é pesado → lote menor = mais seguro


# ==========================================================
# GET ORDERS IDS
# ==========================================================
def get_orders_ids_from_db():
    try:
        if not gb_daily:
            query = """
                SELECT DISTINCT ora.orderid    
                FROM orders_list ora   
                LEFT JOIN orders ord ON ora.orderid = ord.orderid
                WHERE ord.orderid IS NULL
            """
        else:
            query = """
                SELECT DISTINCT ora.orderid    
                FROM orders_list ora      
                WHERE is_change = true
            """

        result = WriteJsonToPostgres(data_conection_info, query, "orders_list").query()
        return result

    except Exception as e:
        logging.error(f"Error in get_orders_ids_from_db: {e}")
        raise


# ==========================================================
# API GET – Order by ID
# ==========================================================
def get_order_by_id(orderId):

    try:
        order_id_value = orderId[0]    # extraído do tuple retornado do DB

        return make_request(
            api_conection_info["Domain"],
            "GET",
            f"api/oms/pvt/orders/{order_id_value}",
            params=None,
            headers=api_conection_info["headers"],
        )

    except Exception as e:
        logging.error(f"Erro ao buscar order {orderId}: {e}")
        raise


# ==========================================================
# PRODUCER: Pega ordem e coloca no buffer
# ==========================================================
def process_order(order_id):
    try:
        order_json = get_order_by_id(order_id)

        if order_json:
            with buffer_lock:
                buffer.append(order_json)

        else:
            logging.error(f"Order não encontrada: {order_id}")

        # Proteção contra limites VTEX OMS
        time.sleep(0.04)  # 40ms entre requisições → seguro

    except Exception as e:
        logging.error(f"Erro ao processar order {order_id}: {e}")
        raise


# ==========================================================
# BATCH SAVE
# ==========================================================
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
        logging.info(f"Salvando batch de {len(batch)} orders...")

        writer = WriteJsonToPostgres(
            data_conection_info,
            batch,
            "orders",
            "orderid"
        )

        writer.upsert_data_batch_otimizado(isdatainsercao=1)

        logging.info("Batch salvo com sucesso.")

    except Exception as e:
        logging.error(f"Erro salvando batch: {e}")
        raise


# ==========================================================
# PROCESSAMENTO PRINCIPAL
# ==========================================================
def process_orders():

    try:
        orders_ids = get_orders_ids_from_db()

        if not orders_ids or not orders_ids[0]:
            logging.info("Nenhuma order pendente encontrada.")
            return True

        id_list = orders_ids[0]

        logging.info(f"Total de orders pendentes: {len(id_list)}")

        # Workers limitados para não estourar OMS
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:

            futures = []

            for order_id in id_list:
                future = executor.submit(process_order, order_id)
                futures.append(future)

                # Salva lote quando uma thread termina
                future.add_done_callback(lambda f: save_batch_if_needed())

            # Espera todas terminarem
            for future in futures:
                future.result()

        # Salva o que sobrou
        save_batch_if_needed(force=True)

        return True

    except Exception as e:
        logging.error(f"Erro inesperado em process_orders: {e}")
        raise


# ==========================================================
# SET GLOBALS
# ==========================================================
def set_globals(api_info, data_conection, coorp_conection, isdaily, **kwargs):
    global api_conection_info, data_conection_info, coorp_conection_info, gb_daily

    api_conection_info = api_info
    data_conection_info = data_conection
    coorp_conection_info = coorp_conection
    gb_daily = isdaily

    process_orders()
