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
buffer_lock = Lock()
BATCH_SIZE = 500   # client_profile é pequeno → lote seguro


# ==========================================================
# ADICIONA AO BUFFER
# ==========================================================
def add_to_buffer(item):
    try:
        with buffer_lock:
            buffer.append(item)
    except Exception as e:
        logging.error(f"Erro ao adicionar item no buffer: {e}")
        raise


# ==========================================================
# SALVA BATCH
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
        logging.info(f"💾 Salvando batch de {len(batch)} registros em client_profile...")

        writer = WriteJsonToPostgres(
            data_conection_info,
            batch,
            "client_profile",
            "orderid",
        )
        writer.upsert_data_batch_otimizado(isdatainsercao=1)

        logging.info(f"Batch de {len(batch)} client_profile salvo com sucesso.")

    except Exception as e:
        logging.error(f"Erro ao salvar batch client_profile: {e}")
        raise


# ==========================================================
# PROCESSA UM REGISTRO (PRODUCER)
# ==========================================================
def process_client_profile(result):
    try:
        order_id, client_profile = result

        if client_profile is None:
            logging.warning(f"client_profile vazio para order {order_id}")
            return

        client_profile["orderid"] = order_id

        add_to_buffer(client_profile)

    except Exception as e:
        logging.error(f"Erro ao processar client_profile orderid {order_id}: {e}")
        raise


# ==========================================================
# PROCESSAMENTO PRINCIPAL EM LOOP
# ==========================================================
def write_client_profile_to_database(batch_size=600):
    try:
        while True:

            query = f"""
                WITH max_data_insercao AS (
                    SELECT oi.orderid, MAX(oi.data_insercao) AS max_data_insercao
                    FROM client_profile oi
                    GROUP BY oi.orderid
                )
                SELECT o.orderid, o.clientprofiledata
                FROM orders o
                INNER JOIN orders_list ol ON ol.orderid = o.orderid
                LEFT JOIN max_data_insercao mdi ON mdi.orderid = o.orderid
                WHERE ol.is_change = TRUE
                  AND o.data_insercao > COALESCE(mdi.max_data_insercao, '1900-01-01')
                ORDER BY o.sequence
                LIMIT {batch_size};
            """

            writer = WriteJsonToPostgres(
                data_conection_info, query, "client_profile"
            )
            result = writer.query()

            if not result or not result[0]:
                logging.info("Nenhum client_profile adicional para processar.")
                break

            rows = result[0]

            # PRODUCER → multithread
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = [
                    executor.submit(process_client_profile, row)
                    for row in rows
                ]

                for future in concurrent.futures.as_completed(futures):
                    future.result()  # força erro no airflow se falhar

            # CONSUMER → salva batch
            save_batch_if_needed()

        # flush final
        save_batch_if_needed(force=True)

    except Exception as e:
        logging.error(f"Erro fatal no processamento do client_profile: {e}")
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
        raise ValueError("Parâmetros globais incompletos.")

    write_client_profile_to_database()
