import concurrent.futures
import logging
import math
from threading import Lock
from modules.dbpgconn import WriteJsonToPostgres

# ==========================================================
# GLOBALS
# ==========================================================
api_conection_info = None
data_conection_info = None
coorp_conection_info = None

buffer = []
processed_unique_ids = set()
buffer_lock = Lock()
BATCH_SIZE = 800



# ==========================================================
# PRODUCER — Adiciona ao buffer sem duplicados
# ==========================================================
def add_item_to_buffer(item):
    try:
        unique_id = item.get("uniqueid") or item.get("uniqueId")

        if not unique_id:
            raise ValueError("Item sem uniqueId detectado!")

        with buffer_lock:
            if unique_id in processed_unique_ids:
                return  # ignora duplicados

            processed_unique_ids.add(unique_id)
            buffer.append(item)

    except Exception as e:
        logging.error(f"Erro adicionando item ao buffer: {e}")
        raise


# ==========================================================
# CONSUMER — Salva o batch
# ==========================================================
def save_batch_if_needed(force=False):
    global buffer, processed_unique_ids

    with buffer_lock:
        if len(buffer) < BATCH_SIZE and not force:
            return

        if force:
            batch = buffer[:]
            buffer.clear()
            processed_unique_ids.clear()
        else:
            batch = buffer[:BATCH_SIZE]
            del buffer[:BATCH_SIZE]

            # remove IDs já enviados
            sent_ids = {item["uniqueid"] for item in batch}
            processed_unique_ids -= sent_ids

    if not batch:
        return

    try:
        logging.info(f"🔄 Salvando batch de {len(batch)} order_items...")

        writer = WriteJsonToPostgres(
            data_conection_info,
            batch,
            "orders_items",
            "uniqueid"
        )
        writer.upsert_data_batch_otimizado(isdatainsercao=1)

        logging.info(f"Batch de {len(batch)} itens salvo com sucesso.")

    except Exception as e:
        logging.error(f"Erro ao salvar batch orders_items: {e}")
        raise


# ==========================================================
# PROCESSAMENTO PRINCIPAL
# ==========================================================
def write_orders_item_to_database():
    try:
        logging.info("🔍 Iniciando carregamento único de orders para processamento...")

        # -----------------------------------------------------------
        # SELECT executado apenas 1 vez — TOTAL das orders pendentes
        # -----------------------------------------------------------
        # query = """
        #     WITH max_data_insercao AS (
        #         SELECT oi.orderid, MAX(oi.data_insercao) AS max_data_insercao
        #         FROM orders_items oi
        #         GROUP BY oi.orderid
        #     )
        #     SELECT o.orderid, o.items
        #     FROM orders o
        #     INNER JOIN orders_list ol ON ol.orderid = o.orderid
        #     LEFT JOIN max_data_insercao mdi ON mdi.orderid = o.orderid
        #     WHERE ol.is_change = TRUE
        #       AND o.data_insercao > COALESCE(mdi.max_data_insercao, '1900-01-01')
        #     ORDER BY o.sequence
        # """

        query = """
            select o.orderid,o.items	from orders  o
            left join orders_items oi on 
            oi.orderid = o.orderid
            where oi.orderid is null ;
        """

        writer = WriteJsonToPostgres(data_conection_info, query, "orders_items")
        result = writer.query()

        if not result or not result[0]:
            logging.info("Nenhum registro pendente para processar order_items.")
            return

        rows = result[0]
        total_orders = len(rows)

        logging.info(f"📦 Total orders pendentes encontradas: {total_orders}")

        # -----------------------------------------------------------
        # PROCESSAMENTO DOS ITEMS — PRODUCER + BUFFER
        # -----------------------------------------------------------
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = []

            for order in rows:
                orderid = order[0]
                items = order[1]

                for item in items:
                    item["orderid"] = orderid
                    futures.append(executor.submit(add_item_to_buffer, item))

            # Espera o processamento de todos os items
            for future in concurrent.futures.as_completed(futures):
                future.result()

        # -----------------------------------------------------------
        # SALVA o que sobrou no buffer
        # -----------------------------------------------------------
        save_batch_if_needed(force=True)

        # -----------------------------------------------------------
        # VERIFICAÇÃO FINAL — ORDERS SEM ITEMS DEVEM GERAR ERRO
        # -----------------------------------------------------------
        logging.info("🔎 Verificando consistência: procurando orders sem items...")

        validation_query = """
            select o.orderid	from orders  o
            left join orders_items oi on 
            oi.orderid = o.orderid
            where oi.orderid is null ;
        """

        validator = WriteJsonToPostgres(data_conection_info, validation_query, "orders_items")
        missing = validator.query()

        if missing and missing[0]:
            missing_ids = [row[0] for row in missing[0]]
            logging.error(
                f"❌ ERRO CRÍTICO: As seguintes orders não tiveram items gravados: {missing_ids}"
            )
            raise RuntimeError(
                f"Processamento incompleto! Orders sem items: {missing_ids}"
            )

        logging.info("✅ Consistência OK: todas as orders foram processadas.")

    except Exception as e:
        logging.error(f"Erro inesperado em write_orders_item_to_database: {e}")
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
        raise ValueError("Global connection information is incomplete.")

    write_orders_item_to_database()
