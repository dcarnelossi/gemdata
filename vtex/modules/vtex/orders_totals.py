import concurrent.futures
import logging
from threading import Lock

from modules.dbpgconn import WriteJsonToPostgres

# ==========================================================
# GLOBALS
# ==========================================================
api_conection_info = None
data_conection_info = None
coorp_conection_info = None

buffer = []
processed_ids = set()     # ← evita duplicidade de orderid
buffer_lock = Lock()

BATCH_SIZE = 500   # orders_totals é pequeno → batch eficiente


# ==========================================================
# ADD TO BUFFER (SEM DUPLICADOS)
# ==========================================================
def add_to_buffer(item):
    try:
        orderid = item["orderid"]

        with buffer_lock:
            # evita duplicidade dentro do mesmo batch
            if orderid in processed_ids:
                return

            processed_ids.add(orderid)
            buffer.append(item)

    except Exception as e:
        logging.error(f"Erro ao adicionar item ao buffer: {e}")
        raise


# ==========================================================
# SAVE BATCH
# ==========================================================
def save_batch_if_needed(force=False):
    global buffer, processed_ids

    logging.info(f"💾 Tentando salvar o  batch ...")
    
    with buffer_lock:
        if len(buffer) < BATCH_SIZE and not force:
            return

        if force:
            batch = buffer[:]
            buffer.clear()
            processed_ids.clear()
        else:
            batch = buffer[:BATCH_SIZE]
            del buffer[:BATCH_SIZE]

            # remove ids processados do set
            processed_batch_ids = {item["orderid"] for item in batch}
            processed_ids -= processed_batch_ids

    if not batch:
        return

    try:
        logging.info(f"💾 Salvando batch de {len(batch)} registros (orders_totals)...")

        writer = WriteJsonToPostgres(
            data_conection_info,
            batch,
            "orders_totals",
            "orderid"
        )
        writer.upsert_data_batch_otimizado(isdatainsercao=1)

        logging.info(f"Batch de {len(batch)} orders_totals salvo com sucesso.")

    except Exception as e:
        logging.error(f"Erro ao salvar batch orders_totals: {e}")
        raise


# ==========================================================
# PROCESSA 1 ITEM (PRODUCER)
# ==========================================================
def process_order_item_colunar(order_totals):
    try:
        order_id, totals_list = order_totals

        result = {"orderid": order_id}


        for item in totals_list:

            # totals padrão
            result[item["id"]] = item["value"]

            # totals alternativos
            if "alternativeTotals" in item:
                for alt_total in item["alternativeTotals"]:
                    alt_id = alt_total["id"]
                    alt_value = alt_total["value"]
                    result[alt_id] = alt_value

        # adiciona ao buffer (sem duplicar orderid)
        add_to_buffer(result)

    except Exception as e:
        logging.error(f"Erro ao processar order_totals para orderid {order_id}: {e}")
        raise


# ==========================================================
# PROCESSAMENTO PRINCIPAL
# ==========================================================
def write_orders_totals_to_database_colunar(batch_size=600):
    try:
        while True:

            # 🔥 ESSENCIAL: limpar ids antes de cada leitura da query
            processed_ids.clear()

            query = f"""
                WITH max_data_insercao AS (
                    SELECT oi.orderid, MAX(oi.data_insercao) AS max_data_insercao
                    FROM orders_totals oi
                    GROUP BY oi.orderid
                )
                SELECT o.orderid, o.totals
                FROM orders o
                INNER JOIN orders_list ol ON ol.orderid = o.orderid
                LEFT JOIN max_data_insercao mdi ON mdi.orderid = o.orderid
                WHERE ol.is_change = TRUE
                  AND o.data_insercao > COALESCE(mdi.max_data_insercao, '1900-01-01')
                ORDER BY o.sequence
                LIMIT {batch_size};
            """

            rows = WriteJsonToPostgres(
                data_conection_info, query, "orders_totals"
            ).query()

            if not rows or not rows[0]:
                logging.info("Nenhum orders_totals adicional para processar.")
                break

            rows = rows[0]

            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(process_order_item_colunar, row) for row in rows]

                for future in concurrent.futures.as_completed(futures):
                    future.result()

            save_batch_if_needed()

        save_batch_if_needed(force=True)

    except Exception as e:
        logging.error(f"Erro crítico no processamento orders_totals: {e}")
        raise


# ==========================================================
# SET GLOBALS
# ==========================================================
def set_globals(api_info, data_conection, coorp_conection, **kwargs):
    global api_conection_info, data_conection_info, coorp_conection_info

    api_conection_info = api_info
    data_conection_info = data_conection
    coorp_conection_info = coorp_conection

    if not all([api_conection_info, data_conection_info, coorp_conection_info]):
        raise ValueError("Global connection info is incomplete.")

    write_orders_totals_to_database_colunar()
