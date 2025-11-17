import concurrent.futures
import logging
import time
from datetime import datetime, timedelta
from threading import Lock

from modules.api_conection import make_request
from modules.dbpgconn import WriteJsonToPostgres
from modules.helpers import increment_one_day

# --------------------------------------------------------------
# LOGGING
# --------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)

# --------------------------------------------------------------
# GLOBALS
# --------------------------------------------------------------
api_conection_info = None
data_conection_info = None
coorp_conection_info = None

# --------------------------------------------------------------
# BATCH BUFFER
# --------------------------------------------------------------
order_buffer = []
processed_order_ids = set()
buffer_lock = Lock()
ORDER_BATCH_SIZE = 300   # ajuste conforme necessidade (recomendação: 200~500)


# ==============================================================
# BATCH — Adiciona pedido ao buffer
# ==============================================================
def add_order_to_buffer(order_json):
    try:
        order_id = order_json.get("orderId")
        if not order_id:
            logging.error("⚠️ Pedido retornado da API sem orderId!")
            return

        with buffer_lock:
            if order_id in processed_order_ids:
                return  # evita duplicidade

            processed_order_ids.add(order_id)
            order_buffer.append(order_json)

    except Exception as e:
        logging.error(f"Erro adicionando pedido ao buffer: {e}")
        raise


# ==============================================================
# BATCH — Salva lote no banco
# ==============================================================
def save_order_batch(force=False):
    global order_buffer, processed_order_ids

    with buffer_lock:
        if len(order_buffer) < ORDER_BATCH_SIZE and not force:
            return

        batch = order_buffer[:]
        order_buffer.clear()
        processed_order_ids.clear()

    if not batch:
        return

    logging.info(f"💾 Salvando batch de {len(batch)} pedidos em realtime_vtex_orders...")

    try:
        writer = WriteJsonToPostgres(
            data_conection_info,
            batch,
            "realtime_vtex_orders",
            "orderid"
        )
        writer.upsert_data_batch_otimizado(isdatainsercao=1)

        logging.info(f"✅ Batch de {len(batch)} pedidos salvo com sucesso.")

    except Exception as e:
        logging.error(f"❌ Erro ao salvar batch de pedidos: {e}")
        raise


# =============================================================================
# API VTEX OMS
# =============================================================================
def get_order_by_id(order_id: str):
    """
    Busca JSON completo da order no OMS.
    """
    return make_request(
        api_conection_info["Domain"],
        "GET",
        f"api/oms/pvt/orders/{order_id}",
        headers=api_conection_info["headers"]
    )


def get_orders_list_pages(query_params: dict):
    """
    Lista orders pelo endpoint paginado.
    """
    return make_request(
        api_conection_info["Domain"],
        "GET",
        "api/oms/pvt/orders",
        params=query_params,
        headers=api_conection_info["headers"]
    )


# =============================================================================
# Paginação — SOMENTE orders_list
# =============================================================================
def process_page(query_params: dict):
    """
    Pagina orders_list coletando apenas orderIds.
    """
    order_ids = []
    per_page = int(query_params.get("per_page", 100))
    page = 1

    while True:
        params = dict(query_params)
        params["page"] = page
        params["per_page"] = per_page

        logging.info(f"📃 Processando página {page} da listagem...")

        tentativa = 0
        resp = None

        while tentativa < 5 and (resp is None or "list" not in resp):
            try:
                resp = get_orders_list_pages(params)
            except Exception as e:
                logging.warning(f"Falha na tentativa {tentativa+1}/5: {e}")
            finally:
                if resp is None or "list" not in resp:
                    time.sleep(1.5 + tentativa)
                    tentativa += 1

        if not resp or "list" not in resp:
            logging.error(f"❌ Falha ao carregar página {page}: {params}")
            break

        lista = resp.get("list") or []
        ids = [o.get("orderId") for o in lista if o.get("orderId")]

        order_ids.extend(ids)

        paging = resp.get("paging") or {}
        current = int(paging.get("currentPage", page))
        total = int(paging.get("pages", page))

        logging.info(f"📦 Página {current}/{total}: {len(ids)} pedidos")

        if current >= total:
            break

        page += 1
        time.sleep(0.2)

    return order_ids


# =============================================================================
# Processa orders completas em paralelismo + batch
# =============================================================================
def process_orders(order_ids):
    if not order_ids:
        return

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:

            future_map = {
                executor.submit(get_order_by_id, oid): oid
                for oid in order_ids
            }

            for future in concurrent.futures.as_completed(future_map):
                oid = future_map[future]
                try:
                    order_json = future.result()
                    add_order_to_buffer(order_json)
                except Exception as e:
                    logging.error(f"❌ Erro ao baixar pedido {oid}: {e}")
                    continue

        save_order_batch()

    except Exception as e:
        logging.error(f"Erro grave no processamento paralelo: {e}")
        raise


# =============================================================================
# Loop diário
# =============================================================================
def process_orders_lists(start_date, end_date):
    try:
        start_dt, end_dt = validate_and_convert_dates(start_date, end_date)

        while start_dt <= end_dt:

            start_dia, end_dia = increment_one_day(start_dt)

            logging.info(f"📅 Buscando pedidos entre {start_dia} e {end_dia}...")

            qs = {
                "per_page": 100,
                "f_creationDate": f"creationDate:[{start_dia} TO {end_dia}]"
            }

            order_ids = process_page(qs)

            logging.info(f"🔍 {len(order_ids)} pedidos encontrados no dia.")

            if order_ids:
                process_orders(order_ids)

            save_order_batch(force=True)

            start_dt += timedelta(days=1)

        save_order_batch(force=True)

    except Exception as e:
        logging.error(f"Erro inesperado no process_orders_lists: {e}")
        raise


# =============================================================================
# Datas
# =============================================================================
def validate_and_convert_dates(start_date, end_date):
    if not isinstance(start_date, datetime):
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
    if not isinstance(end_date, datetime):
        end_date = datetime.strptime(end_date, "%Y-%m-%d")
    return start_date, end_date


# =============================================================================
# SET GLOBALS
# =============================================================================
def set_globals(api_info, data_conection, coorp_conection, **kwargs):
    global api_conection_info, data_conection_info, coorp_conection_info

    api_conection_info = api_info
    data_conection_info = data_conection
    coorp_conection_info = coorp_conection

    process_orders_lists(kwargs["start_date"], kwargs["end_date"])
