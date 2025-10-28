import concurrent.futures
import logging
import time
from datetime import datetime, timedelta

from modules.api_conection import make_request
from modules.dbpgconn import WriteJsonToPostgres
from modules.helpers import increment_one_day

# Variáveis globais
api_conection_info = None
data_conection_info = None
coorp_conection_info = None
isdaily = None


# =====================================
# CHAMADA À API (ORDERS)
# =====================================
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


# =====================================
# PAGINAÇÃO
# =====================================
def paginate_orders(base_params, per_page=100, max_retries=5, backoff_base=0.5):
    """
    Itera sobre todas as páginas retornadas pela API VTEX OMS,
    usando os metadados do campo 'paging'.
    Faz retry com backoff exponencial em caso de falhas.
    """
    page = 1
    total_pages = None

    while True:
        params = dict(base_params)
        params["per_page"] = per_page
        params["page"] = page

        tentativa, resp = 0, None
        while (resp is None or "list" not in resp) and tentativa < max_retries:
            try:
                resp = get_orders_list_pages(params)
                if resp and "list" in resp:
                    break
            except Exception as e:
                logging.warning(f"Tentativa {tentativa+1}/{max_retries} falhou na página {page}: {e}")
            time.sleep(backoff_base * (2 ** tentativa))
            tentativa += 1

        if not resp or "list" not in resp:
            logging.warning(f"Sem dados ou resposta inválida na página {page}. Encerrando.")
            break

        paging = resp.get("paging", {})
        if total_pages is None:
            total_pages = paging.get("pages", 1)
            per_page_api = paging.get("perPage")
            if per_page_api and per_page_api != per_page:
                logging.info(f"A API retornou perPage={per_page_api}; ajustando.")
                per_page = per_page_api

        orders_list = resp.get("list", [])
        if not orders_list:
            break

        for order in orders_list:
            yield order

        current_page = paging.get("currentPage", page)
        if current_page >= total_pages:
            break

        page += 1


# =====================================
# PROCESSAMENTO DE PEDIDOS
# =====================================
def process_order(order):
    try:
        table = "orders_list_daily" if isdaily else "orders_list"
        writer = WriteJsonToPostgres(data_conection_info, order, table, "orderid")
        writer.upsert_data2(isdatainsercao=1)
        logging.info(f"Order {order['orderId']} upserted successfully.")
    except Exception as e:
        logging.error(f"Error inserting order {order.get('orderId', '<sem id>')}: {e}")
        raise


# =====================================
# LOOP PRINCIPAL DE DIAS E PÁGINAS
# =====================================
def process_orders_lists(start_date, end_date):
    try:
        data_inicial, data_final = validate_and_convert_dates(start_date, end_date)

        while data_inicial <= data_final:
            start_iso, end_iso = increment_one_day(data_inicial)
            qs_base = {
                "f_creationDate": f"creationDate:[{start_iso} TO {end_iso}]"
            }

            logging.info(f"Processando pedidos de {start_iso} até {end_iso}...")

            # Processa todos os pedidos paginados neste intervalo
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                futures = [
                    executor.submit(process_order, order)
                    for order in paginate_orders(qs_base, per_page=100)
                ]
                for future in concurrent.futures.as_completed(futures):
                    future.result()

            data_inicial += timedelta(days=1)

    except Exception as e:
        logging.error(f"Erro inesperado ao processar pedidos: {e}")
        raise


# =====================================
# UTILITÁRIOS E CONTROLE
# =====================================
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


def set_globals(api_info, data_conection, coorp_conection, **kwargs):
    global api_conection_info, data_conection_info, coorp_conection_info, isdaily
    api_conection_info = api_info
    data_conection_info = data_conection
    coorp_conection_info = coorp_conection
    isdaily = kwargs["isdaily"]

    if not all([api_conection_info, data_conection_info, coorp_conection_info]):
        logging.error("Global connection information is incomplete.")
        raise ValueError("All global connection information must be provided.")

    execute_process_orders_list(kwargs["start_date"], kwargs["end_date"])


def execute_process_orders_list(start_date, end_date, delta=None):
    try:
        start_time = time.time()

        if delta:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=delta)

        process_orders_lists(start_date, end_date)
        logging.info("Script executed successfully.")

    except Exception as e:
        logging.error(f"Script encountered an error: {e}")
        raise
    finally:
        logging.info(f"Total execution time: {time.time() - start_time:.2f} seconds")
