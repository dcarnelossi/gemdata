import concurrent.futures
import logging

from modules.api_conection import make_request
from modules.dbpgconn import WriteJsonToPostgres

# Variáveis globais
api_conection_info = None
data_conection_info = None
coorp_conection_info = None


def get_orders_ids_from_db():
    try:
        query = """SELECT DISTINCT orders_list.orderid
                   FROM orders_list
                   LEFT JOIN orders ON orders_list.orderid = orders.orderid
                   WHERE orders.orderid IS NULL;"""
        result = WriteJsonToPostgres(data_conection_info, query, "orders_list").query()
        if not result:
            logging.warning("No order IDs found in the database.")
        return result
    except Exception as e:
        logging.error(f"An error occurred in get_orders_ids_from_db: {e}")
        raise  # Ensure the Airflow task fails on error


def get_order_by_id(order_id):
    try:
        return make_request(
            api_conection_info["VTEX_Domain"],
            "GET",
            f"api/oms/pvt/orders/{order_id[0][0]}",
            headers=api_conection_info["headers"],
        )
    except Exception as e:
        logging.error(f"An error occurred in get_order_by_id for order_id {order_id[0][0]}: {e}")
        raise  # Ensure the Airflow task fails on error


def write_orders_to_db(order_id):
    try:
        order_json = get_order_by_id(order_id)
        writer = WriteJsonToPostgres(
            data_conection_info, order_json, "orders", "orderid"
        )
        writer.upsert_data()
        logging.info(f"Created record for order ID: {order_id[0]}")
    except Exception as e:
        logging.error(f"Error inserting order {order_id[0]} into the database: {e}")
        raise  # Ensure the Airflow task fails on error


def process_orders():
    try:
        orders_ids = get_orders_ids_from_db()
        if not orders_ids:
            logging.warning("No orders to process.")
            return

        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(write_orders_to_db, orders_ids)

    except Exception as e:
        logging.error(f"An error occurred in process_orders: {e}")
        raise  # Ensure the Airflow task fails on error


def set_globals(api_info, data_conection, coorp_conection, **kwargs):
    global api_conection_info, data_conection_info, coorp_conection_info
    api_conection_info = api_info
    data_conection_info = data_conection
    coorp_conection_info = coorp_conection

    if not all([api_conection_info, data_conection_info, coorp_conection_info]):
        logging.error("Global connection information is incomplete.")
        raise ValueError("All global connection information must be provided.")

    process_orders()

# if __name__ == "__main__":
#     set_globals(
#         {"api_key": "example"}, 
#         {"db_url": "postgresql://user:pass@localhost/db"}, 
#         {"coorp_key": "example"}
#     )
