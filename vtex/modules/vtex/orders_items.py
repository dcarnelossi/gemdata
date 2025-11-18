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
processed_unique_ids = set()
buffer_lock = Lock()
BATCH_SIZE = 100
current_batch_number = 0


# ==========================================================
# PRODUCER — Adiciona ao buffer sem duplicados
# ==========================================================
def add_item_to_buffer(item):
    try:
        if not isinstance(item, dict):
            logging.warning(f"Item inválido ignorado (não é dict): {item}")
            return

        unique_id = item.get("uniqueid") or item.get("uniqueId")

        # DESCARTA NULL, vazio, None, ou inexistente
        if not unique_id:
            logging.warning(f"Item ignorado por uniqueId nulo: {item}")
            return

        with buffer_lock:
            if unique_id in processed_unique_ids:
                return  # ignora duplicados

            processed_unique_ids.add(unique_id)
            buffer.append(item)

    except Exception as e:
        logging.error(f"Erro adicionando item ao buffer: {e}")
        raise


# ==========================================================
# CONSUMER — Salva batches corretamente
# ==========================================================
def save_batch_if_needed(force=False):
    """
    Salva o buffer em batches e mostra logs detalhados de cada batch.
    """
    global buffer, processed_unique_ids, current_batch_number

    with buffer_lock:
        if len(buffer) < BATCH_SIZE and not force:
            return

        if force:
            batch = buffer[:]        # pega tudo
            buffer.clear()
            processed_unique_ids.clear()
            is_final = True
        else:
            batch = buffer[:BATCH_SIZE]
            del buffer[:BATCH_SIZE]

            sent_ids = {item["uniqueid"] for item in batch}
            processed_unique_ids -= sent_ids
            is_final = False

    if not batch:
        return

    current_batch_number += 1

    batch_label = (
        f"FINAL ({current_batch_number})" if is_final else f"{current_batch_number}"
    )

    try:
        logging.info(f"🔄 Salvando BATCH {batch_label} com {len(batch)} itens...")

        writer = WriteJsonToPostgres(
            data_conection_info,
            batch,
            "orders_items",
            "uniqueid"
        )
        writer.upsert_data_batch_otimizado(isdatainsercao=1)

        logging.info(f"🟢 Batch {batch_label} salvo com sucesso ({len(batch)} itens).")

    except Exception as e:
        logging.error(f"❌ ERRO ao salvar batch {batch_label}: {e}")
        raise


# ==========================================================
# PROCESSAMENTO PRINCIPAL
# ==========================================================
def write_orders_item_to_database():
    try:
        logging.info("🔍 Iniciando carregamento único de orders para processamento...")

        # -----------------------------------------------------------
        # SELECT executado apenas uma vez
        # -----------------------------------------------------------
        query = """
            SELECT o.orderid, o.items
            FROM orders o
            LEFT JOIN orders_items oi ON oi.orderid = o.orderid
            WHERE oi.orderid IS NULL;
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
        # PROCESSAMENTO — PRODUCER + BUFFER COM BATCHING
        # -----------------------------------------------------------
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = []

            for order in rows:
                orderid = order[0]
                items = order[1]

                for item in items:
                    item["orderid"] = orderid

                    futures.append(executor.submit(add_item_to_buffer, item))

                    # 🔥 BATCHING OCORRE AQUI!
                    save_batch_if_needed()  # salva quando chegar aos 500

            # Espera threads terminarem
            for future in concurrent.futures.as_completed(futures):
                future.result()

        # -----------------------------------------------------------
        # SALVA RESTANTE DO BUFFER
        # -----------------------------------------------------------
        save_batch_if_needed(force=True)

        # -----------------------------------------------------------
        # VERIFICAÇÃO FINAL
        # -----------------------------------------------------------
        logging.info("🔎 Verificando consistência: procurando orders sem items...")

        validation_query = """
            SELECT o.orderid
            FROM orders o
            LEFT JOIN orders_items oi ON oi.orderid = o.orderid
            WHERE oi.orderid IS NULL;
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
