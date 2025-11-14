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
buffer_lock = Lock()
BATCH_SIZE = 800   # items são pequenos → lote maior é seguro


# ==========================================================
# Criar tabela caso não exista
# ==========================================================
def create_orders_items_database(table_name):
    try:
        writer = WriteJsonToPostgres(data_conection_info, "{}", table_name)

        if not writer.table_exists():
            query = f"""
                SELECT orders.orderid, orders.{table_name}
                FROM orders
                LIMIT 1
            """
            result = writer.query()

            if not result or not result[0]:
                raise ValueError(f"No data found to create table '{table_name}'.")

            dados = result[0][1]

            # Adiciona orderid dentro de cada item
            for item in dados:
                item["orderid"] = result[0][0]

            writer = WriteJsonToPostgres(data_conection_info, dados, table_name)
            writer.create_table()

            logging.info(f"Table '{table_name}' created successfully.")
        else:
            logging.info(f"Table '{table_name}' already exists.")

    except Exception as e:
        logging.error(f"Error creating table '{table_name}': {e}")
        raise


# ==========================================================
# PRODUCER — Extrai e joga no buffer
# ==========================================================
def add_item_to_buffer(item):
    try:
        with buffer_lock:
            buffer.append(item)
    except Exception as e:
        logging.error(f"Erro adicionando item ao buffer: {e}")
        raise


# ==========================================================
# CONSUMER — Salva lote no banco
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
# PROCESSAMENTO PRINCIPAL EM BATCHES
# ==========================================================
def write_orders_item_to_database(batch_size=400):
    try:
        # 1) Conta quantos registros processar
        count_query = """
            SELECT COUNT(*)
            FROM orders o
            WHERE o.orderid IN (
                SELECT orderid
                FROM orders_list
                WHERE is_change = TRUE
            )
        """

        count_writer = WriteJsonToPostgres(data_conection_info, count_query, "orders_items")
        records = count_writer.query()

        if not records or not records[0]:
            logging.info("Nenhum registro para processar order_items.")
            return

        total_records = records[0][0][0]
        total_batches = math.ceil(total_records / batch_size)

        logging.info(f"Total order_items a processar: {total_records} → {total_batches} batches")

        # 2) Loop de batches
        for batch_num in range(total_batches):

            query = f"""
                WITH max_data_insercao AS (
                    SELECT oi.orderid, MAX(oi.data_insercao) AS max_data_insercao
                    FROM orders_items oi
                    GROUP BY oi.orderid
                )
                SELECT o.orderid, o.items
                FROM orders o
                INNER JOIN orders_list ol ON ol.orderid = o.orderid
                LEFT JOIN max_data_insercao mdi ON mdi.orderid = o.orderid
                WHERE ol.is_change = TRUE
                  AND o.data_insercao > COALESCE(mdi.max_data_insercao, '1900-01-01')
                ORDER BY o.sequence
                LIMIT {batch_size}
            """

            batch_writer = WriteJsonToPostgres(data_conection_info, query, "orders_items")
            result = batch_writer.query()

            if not result or not result[0]:
                logging.info("Nenhum dado adicional após batch %s", batch_num)
                break

            rows = result[0]

            # PRODUCER → captura items e joga no buffer
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = []

                for order in rows:
                    orderid = order[0]
                    items = order[1]

                    for item in items:
                        item["orderid"] = orderid
                        futures.append(executor.submit(add_item_to_buffer, item))

                for future in concurrent.futures.as_completed(futures):
                    future.result()

            # CONSUMER → grava lote
            save_batch_if_needed()

        # Finaliza buffer
        save_batch_if_needed(force=True)

    except Exception as e:
        logging.error(f"Unexpected error in write_orders_item_to_database: {e}")
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
