import concurrent.futures
import logging

from modules.dbpgconn import WriteJsonToPostgres

api_conection_info = None
data_conection_info = None
coorp_conection_info = None


def write_client_profile_to_database(batch_size=600):
    try:
        while True:
            # Query dinâmica
            query = f"""
                        WITH max_data_insercao AS (
                            SELECT oi.orderid, MAX(oi.data_insercao) AS max_data_insercao
                            FROM client_profile oi
                            GROUP BY oi.orderid
                        )
                        SELECT  o.orderid ,o.clientprofiledata 
                        FROM orders o
                        INNER JOIN orders_list ol ON ol.orderid = o.orderid
                        LEFT JOIN max_data_insercao mdi ON mdi.orderid = o.orderid
                        WHERE ol.is_change = TRUE
                        AND o.data_insercao > COALESCE(mdi.max_data_insercao, '1900-01-01')
                        ORDER BY o.sequence
                        LIMIT {batch_size};"""
            result = WriteJsonToPostgres(
                data_conection_info, query, "client_profile"
            ).query()

            if not result[0]:
                break  # No more results, exit the loop

            try:
                client_ids = result[0]
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future_to_order = {executor.submit(process_client_profile, client_id): client_id for client_id in client_ids[0]}
                    for future in concurrent.futures.as_completed(future_to_order):
                        client_id = future_to_order[future]
                        try:
                            future.result()  # This will raise an exception if the thread failed
                        except Exception as e:
                            logging.error(f"Order {client_id} generated an exception: {e}")
                            raise e  # Raise the exception to ensure task failure
                return True
            except Exception as e:
                logging.error(f"An unexpected error occurred: {e}")
                raise e    
                    

        #     with concurrent.futures.ThreadPoolExecutor() as executor:
        #         # Mapeia a função para cada item em result usando threads
        #         list(executor.map(process_client_profile, result[0]))

        # return True

    except Exception as e:
        logging.error(f"write_client_profile_to_database - Erro desconhecido - {e}")
        raise e


def process_client_profile(result):
    try:
        order_id, client_profile = result
        client_profile["orderid"] = order_id

        writer = WriteJsonToPostgres(
            data_conection_info, client_profile, "client_profile", "orderid"
        )
        writer.upsert_data2(isdatainsercao=1)
        logging.info(f"Inserção de dados concluída para orderid - {order_id}")

        return True  # Indica que a execução foi bem-sucedida

    except Exception as e:
        error_message = f"Erro ao processar item - {e}"
        logging.error(error_message)
        print(error_message)
        raise e


def set_globals(api_info, data_conection, coorp_conection, **kwargs):
    global api_conection_info
    api_conection_info = api_info

    global data_conection_info
    data_conection_info = data_conection

    global coorp_conection_info
    coorp_conection_info = coorp_conection

    write_client_profile_to_database()


# if __name__ == "__main__":
#     write_client_profile_to_database()
