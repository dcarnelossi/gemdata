import concurrent.futures
import logging
from threading import Lock

from modules.dbpgconn import WriteJsonToPostgres
from modules.helpers import flatten_json

# ==========================================================
# GLOBALS
# ==========================================================
api_conection_info = None
data_conection_info = None
coorp_conection_info = None

buffer = []
processed_ids = set()
buffer_lock = Lock()

BATCH_SIZE = 500


# ==========================================================
# ADD TO BUFFER (SEM DUPLICADOS)
# ==========================================================
def add_to_buffer(item):
    try:
        orderid = item["orderid"]

        with buffer_lock:
            # evita duplicidade no mesmo batch
            if orderid in processed_ids:
                return

            processed_ids.add(orderid)
            buffer.append(item)

    except Exception as e:
        logging.error(f"Erro ao adicionar ao buffer: {e}")
        raise


# ==========================================================
# SALVAR BATCH
# ==========================================================
def save_batch_if_needed(force=False):
    global buffer, processed_ids

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

            # remove ids inseridos do set
            processed_ids -= {item["orderid"] for item in batch}

    if not batch:
        return

    try:
        logging.info(f"🔄 Salvando batch de {len(batch)} registros shippingdata...")

        writer = WriteJsonToPostgres(
            data_conection_info,
            batch,
            "orders_shippingdata",
            "orderid",
        )
        writer.upsert_data_batch_otimizado(isdatainsercao=1)

        logging.info(f"Batch de {len(batch)} shippingdata salvo com sucesso.")

    except Exception as e:
        logging.error(f"Erro ao salvar batch de shippingdata: {e}")
        raise


# ==========================================================
# PROCESSAMENTO INDIVIDUAL (PRODUCER)
# ==========================================================
def process_shippingdata(order_row):
    try:
        order_id, shippingdata = order_row

        json_data = {
            "selectedAddresses": shippingdata.get("selectedAddresses", []),
            "address": shippingdata.get("address", {}),
            "orderid": order_id,
        }

        flat_json = flatten_json(json_data)

        add_to_buffer(flat_json)

    except Exception as e:
        logging.error(f"Erro ao processar shippingdata (order {order_id}): {e}")
        raise


# ==========================================================
# PROCESSO PRINCIPAL (CONSUMER)
# ==========================================================

def write_orders_shippingdata_to_database(batch_size=500):
    try:
        logging.info("🔍 Iniciando carregamento único de shippingdata para processamento...")

        # -----------------------------------------------------------
        # SELECT executado apenas 1 vez — TOTAL das orders pendentes
        # -----------------------------------------------------------
        query = """
             WITH max_data_insercao AS (
                    SELECT oi.orderid, MAX(oi.data_insercao) AS max_data_insercao
                    FROM orders_shippingdata oi
                    GROUP BY oi.orderid
                )
                SELECT o.orderid, o.shippingdata
                FROM orders o
                INNER JOIN orders_list ol ON ol.orderid = o.orderid
                LEFT JOIN max_data_insercao mdi ON mdi.orderid = o.orderid
                WHERE ol.is_change = TRUE
                  AND o.data_insercao > COALESCE(mdi.max_data_insercao, '1900-01-01')
                ORDER BY o.sequence
        """
        # query = """
        #       select o.orderid,o.shippingdata	from orders  o
        #     left join orders_shippingdata oi on 
        #     oi.orderid = o.orderid
        #     where oi.orderid is null 
        # """

        writer = WriteJsonToPostgres(data_conection_info, query, "orders_shippingdata")
        result = writer.query()

        if not result or not result[0]:
            logging.info("Nenhum shippingdata pendente para processar.")
            return

        rows = result[0]
        total_orders = len(rows)

        logging.info(f"📦 Total orders pendentes encontradas para shippingdata: {total_orders}")

        # -----------------------------------------------------------
        # PROCESSAMENTO DO SHIPPINGDATA — PRODUCER + THREADS
        # -----------------------------------------------------------
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(process_shippingdata, row)
                for row in rows
            ]

            for future in concurrent.futures.as_completed(futures):
                future.result()  # trata exceções das threads
                save_batch_if_needed()

        # -----------------------------------------------------------
        # SALVA o que restou no buffer
        # -----------------------------------------------------------
        save_batch_if_needed(force=True)

        # -----------------------------------------------------------
        # VERIFICAÇÃO FINAL — ORDERS SEM SHIPPINGDATA GERAM ERRO
        # -----------------------------------------------------------
        logging.info("🔎 Verificando consistência: procurando orders sem shippingdata...")

        validation_query = """
            select o.orderid	from orders  o
            left join orders_shippingdata oi on 
            oi.orderid = o.orderid
            where oi.orderid is null 
        """

        validator = WriteJsonToPostgres(data_conection_info, validation_query, "orders_shippingdata")
        missing = validator.query()

        if missing and missing[0]:
            missing_ids = [row[0] for row in missing[0]]
            logging.error(
                f"❌ ERRO CRÍTICO: As seguintes orders não tiveram shippingdata gravados: {missing_ids}"
            )
            raise RuntimeError(
                f"Processamento incompleto! Orders sem shippingdata: {missing_ids}"
            )

        logging.info("✅ Consistência OK: todos os shippingdata foram processados.")

    except Exception as e:
        logging.error(f"Erro fatal no processamento do shippingdata: {e}")
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
        raise ValueError("Missing connection info.")

    write_orders_shippingdata_to_database()
