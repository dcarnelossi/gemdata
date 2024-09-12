import concurrent.futures
import logging

from modules.dbpgconn import WriteJsonToPostgres

# Variáveis globais
api_conection_info = None
data_conection_info = None
coorp_conection_info = None

def write_orders_totals_to_database_colunar(batch_size=600):
    try:
        while True:
            query = f"""
            select o.orderid ,o.totals  from orders o 
	        where o.orderid in (select orderid from orders_list ol where ol.is_change = true )
            LIMIT {batch_size};"""

            result = WriteJsonToPostgres(
                data_conection_info, query, "orders_totals"
            ).query()

            if not result or not result[0]:
                logging.info("No more orders to process. Exiting loop.")
                break  # No more results, exit the loop

            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = {executor.submit(process_order_item_colunar, order_totals): order_totals for order_totals in result[0]}
                for future in concurrent.futures.as_completed(futures):
                    order_totals = futures[future]
                    try:
                        future.result()
                    except Exception as e:
                        logging.error(f"Error processing orderid {order_totals[0]}: {e}")
                        raise  # Propagate the exception to fail the Airflow task

    except Exception as e:
        logging.error(f"Unexpected error in write_orders_totals_to_database_colunar: {e}")
        raise  # Ensure the Airflow task fails on error

def process_order_item_colunar(order_totals):
    try:
        order_id, totals = order_totals
        result = {"orderid": order_id}

        for item in totals:
            result[item["id"]] = item["value"]

            if "alternativeTotals" in item:
                for alt_total in item["alternativeTotals"]:
                    alt_id = alt_total["id"]
                    alt_value = alt_total["value"]
                    if alt_id not in result:
                        result[alt_id] = alt_value

        writer = WriteJsonToPostgres(
            data_conection_info, result, "orders_totals", "orderid"
        )
        writer.upsert_data()

        logging.info(f"Data upserted successfully for orderid - {order_id}")

    except Exception as e:
        logging.error(f"Error processing order totals for orderid {order_id}: {e}")
        raise  # Propagate the exception to fail the Airflow task

def set_globals(api_info, data_conection, coorp_conection, **kwargs):
    global api_conection_info, data_conection_info, coorp_conection_info
    api_conection_info = api_info
    data_conection_info = data_conection
    coorp_conection_info = coorp_conection

    if not all([api_conection_info, data_conection_info, coorp_conection_info]):
        logging.error("Global connection information is incomplete.")
        raise ValueError("All global connection information must be provided.")

    write_orders_totals_to_database_colunar()

# if __name__ == "__main__":
#     set_globals(
#         {"api_key": "example"}, 
#         {"db_url": "postgresql://user:pass@localhost/db"}, 
#         {"coorp_key": "example"}
#     )
