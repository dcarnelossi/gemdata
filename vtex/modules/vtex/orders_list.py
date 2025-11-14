import concurrent.futures
import logging
import time
from datetime import datetime, timedelta
from threading import Lock

from modules.api_conection import make_request
from modules.dbpgconn import WriteJsonToPostgres
from modules.helpers import increment_one_day

# ================================================================
# GLOBALS
# ================================================================
api_conection_info = None
data_conection_info = None
coorp_conection_info = None
isdaily = None

buffer = []
buffer_lock = Lock()
BATCH_SIZE = 500   # OMS é sensível → mantemos 500


# ================================================================
# REQUEST COM RETRY
# ================================================================
def get_orders_list_pages(query_params):
    try:
        return make_request(
            api_conection_info["Domain"],
            "GET",
            "api/oms/pvt/orders",
            params=query_params,
            headers=api_conection_info["headers"],
        )
    except Exception as e:
        logging.error(f"Failed to retrieve orders list pages: {e}")
        raise


# ================================================================
# PAGINAÇÃO
# ================================================================
def paginate_orders(base_params, per_page=100, max_retries=5, backoff_base=0.5):

    page = 1
    total_pages = None

    while True:

        params = dict(base_params)
        params["per_page"] = per_page
        params["page"] = page

        tentativa, resp = 0, None
        while tentativa < max_retries:
            try:
                resp = get_orders_list_pages(params)
                if resp and "list" in resp:
                    break
            except Exception as e:
                logging.warning(
                    f"Tentativa {tentativa+1}/{max_retries} falhou na página {page}: {e}"
                )
            time.sleep(backoff_base * (2 ** tentativa))
            tentativa += 1

        if not resp or "list" not in resp:
            logging.warning(f"Nenhum dado válido retornado na página {page}. Encerrando.")
            break

        paging = resp.get("paging", {})
        if total_pages is None:
            total_pages = paging.get("pages", 1)
            per_page_api = paging.get("perPage")
            if per_page_api and per_page_api != per_page:
                logging.info(f"API retornou perPage={per_page_api}, ajustando.")
                per_page = per_page_api

        for order in resp.get("list", []):
            yield order

        if paging.get("currentPage", page) >= total_pages:
            break

        page += 1


# ================================================================
# BUFFER SAVE (BATCH REAL)
# ================================================================
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
        table = "orders_list_daily" if isdaily else "orders_list"
        logging.info(f"Gravando batch de {len(batch)} orders na tabela {table}...")

        writer = WriteJsonToPostgres(
            data_conection_info,
            batch,
            table,
            "orderid"
        )
        writer.upsert_data_batch_otimizado(isdatainsercao=1)

        logging.info("Batch salvo com sucesso.")

    except Exception as e:
        logging.error(f"Erro ao salvar batch de orders: {e}")
        raise


# ================================================================
# PROCESS ORDER (THREAD PRODUCER)
# ================================================================
def process_order(order):
    try:
        with buffer_lock:
            buffer.append(order)

    except Exception as e:
        logging.error(f"Erro ao adicionar order no buffer ({order.get('orderId')}): {e}")
        raise


# ================================================================
# PROCESSAMENTO PRINCIPAL DOS DIAS
# ================================================================
def process_orders_lists(start_date, end_date):

    data_inicial, data_final = validate_and_convert_dates(start_date, end_date)

    while data_inicial <= data_final:

        start_iso, end_iso = increment_one_day(data_inicial)
        qs_base = {
            "f_creationDate": f"creationDate:[{start_iso} TO {end_iso}]"
        }

        logging.info(f"Processando pedidos de {start_iso} até {end_iso}...")

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:

            futures = []

            for order in paginate_orders(qs_base, per_page=100):

                future = executor.submit(process_order, order)
                futures.append(future)

                # Salva batch quando uma thread termina
                future.add_done_callback(lambda f: save_batch_if_needed())

                # OMS TEM LIMITES BAIXOS!
                time.sleep(0.03)  # 30ms → seguro

            # Aguarda tudo terminar
            for future in futures:
                future.result()

        # salva sobra de orders
        save_batch_if_needed(force=True)

        data_inicial += timedelta(days=1)


# ================================================================
# VALIDATE DATES
# ================================================================
def validate_and_convert_dates(start_date, end_date):
    try:
        if not isinstance(start_date, datetime):
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
        if not isinstance(end_date, datetime):
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
        return start_date, end_date
    except ValueError as e:
        logging.error(f"Invalid date format: {e}")
        raise


# ================================================================
# SET GLOBALS
# ================================================================
def set_globals(api_info, data_conection, coorp_conection, **kwargs):
    global api_conection_info, data_conection_info, coorp_conection_info, isdaily
    api_conection_info = api_info
    data_conection_info = data_conection
    coorp_conection_info = coorp_conection
    isdaily = kwargs.get("isdaily")

    execute_process_orders_list(kwargs["start_date"], kwargs["end_date"])


# ================================================================
# EXECUTE PROCESS
# ================================================================
def execute_process_orders_list(start_date, end_date, delta=None):

    try:
        start_time = time.time()

        if delta:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=delta)

        process_orders_lists(start_date, end_date)
        logging.info("Orders process executed successfully.")

    except Exception as e:
        logging.error(f"Error in Orders: {e}")
        raise

    finally:
        logging.info(f"Total execution time: {time.time() - start_time:.2f}s")
