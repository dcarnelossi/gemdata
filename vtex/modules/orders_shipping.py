import concurrent.futures
import logging

from modules.dbpgconn import WriteJsonToPostgres
from modules.helpers import flatten_json

# Variáveis globais
api_conection_info = None
data_conection_info = None
coorp_conection_info = None

def write_orders_shippingdata_to_database(batch_size=600):
    try:
        while True:
            query = f"""                        
                        WITH max_data_insercao AS (
                            SELECT oi.orderid, MAX(oi.data_insercao) AS max_data_insercao
                            FROM orders_shippingdata oi
                            GROUP BY oi.orderid
                        )
                        SELECT  o.orderid ,o.shippingdata
                        FROM orders o
                        INNER JOIN orders_list ol ON ol.orderid = o.orderid
                        LEFT JOIN max_data_insercao mdi ON mdi.orderid = o.orderid
                        WHERE ol.is_change = TRUE
                        AND o.data_insercao > COALESCE(mdi.max_data_insercao, '1900-01-01')
                        ORDER BY o.sequence
                        LIMIT {batch_size};"""

            result = WriteJsonToPostgres(
                data_conection_info, query, "orders_shippingdata"
            )
            result = result.query()

            if not result or not result[0]:
                logging.info("No more orders to process. Exiting loop.")
                break  # No more results, exit the loop

            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = {executor.submit(process_orders_shippingdata, res): res for res in result[0]}
                for future in concurrent.futures.as_completed(futures):
                    res = futures[future]
                    try:
                        success, error_message = future.result()
                        if not success:
                            logging.error(f"Failed to process shipping data for order {res[0]}: {error_message}")
                            raise Exception(f"Failed to process shipping data for order {res[0]}: {error_message}")
                    except Exception as e:
                        logging.error(f"Error during shipping data processing: {e}")
                        raise  # Rethrow to signal Airflow task failure

    except Exception as e:
        logging.error(f"write_orders_shippingdata_to_database - Unexpected error: {e}")
        raise  # Ensure Airflow registers the task as failed

def process_orders_shippingdata(result):
    try:
        order_id, orders_shippingdata = result

        json = {
            "selectedAddresses": orders_shippingdata.get("selectedAddresses", []),
            "address": orders_shippingdata.get("address", {}),
            "orderid": order_id,
        }

        new_json = flatten_json(json)

        writer = WriteJsonToPostgres(
            data_conection_info, new_json, "orders_shippingdata", "orderid"
        )
        writer.upsert_data(isdatainsercao=1)

        logging.info(f"Data insertion completed for orderid - {order_id}")

        return True, None  # Indicates successful execution

    except Exception as e:
        error_message = f"Error processing orders_shippingdata - {e}"
        logging.error(error_message)
        return False, str(e)  # Indicates a failure in execution with error details

def set_globals(api_info, data_conection, coorp_conection, **kwargs):
    global api_conection_info, data_conection_info, coorp_conection_info
    api_conection_info = api_info
    data_conection_info = data_conection
    coorp_conection_info = coorp_conection

    if not all([api_conection_info, data_conection_info, coorp_conection_info]):
        logging.error("Global connection information is incomplete.")
        raise ValueError("All global connection information must be provided.")

    write_orders_shippingdata_to_database()

# if __name__ == "__main__":
#     set_globals(
#         {"api_key": "example"}, 
#         {"db_url": "postgresql://user:pass@localhost/db"}, 
#         {"coorp_key": "example"}
#     )
