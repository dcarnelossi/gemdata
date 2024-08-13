import concurrent.futures
import logging
import time
from datetime import datetime, timedelta

from modules.api_conection import make_request
from modules.dbpgconn import WriteJsonToPostgres
from modules.helpers import increment_one_day

# Variáveis globais
api_conection_info = None
data_conection_info = None
coorp_conection_info = None


def get_orders_list_pages(query_params):
    try:
        return make_request(
            api_conection_info["VTEX_Domain"],
            "GET",
            "api/oms/pvt/orders",
            params=query_params,
            headers=api_conection_info["headers"],
        )
    except Exception as e:
        logging.error(f"Failed to retrieve orders list pages: {e}")
        raise  # Rethrow the exception to signal the Airflow task failure


def process_page(query_params):
    try:
        logging.info(f"Processing page with params: {query_params}")
        lista = get_orders_list_pages(query_params)

        if not lista or "list" not in lista:
            logging.error(f"No orders found for params: {query_params}")
            raise ValueError(f"Invalid response for params: {query_params}")

        orders_list = lista["list"]

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {executor.submit(process_order, order): order for order in orders_list}
            for future in concurrent.futures.as_completed(futures):
                order = futures[future]
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Error processing order {order['orderId']}: {e}")
                    raise  # Propagate the exception to fail the Airflow task

    except Exception as e:
        logging.error(f"An unexpected error occurred while processing the page: {e}")
        raise  # Ensure any error fails the Airflow task


def process_order(order):
    try:
        writer = WriteJsonToPostgres(
            data_conection_info, order, "orders_list", "orderid"
        )
        writer.upsert_data()
        logging.info(f"Order {order['orderId']} upserted successfully.")
    except Exception as e:
        logging.error(f"Error inserting order {order['orderId']}: {e}")
        raise  # Ensure failure is propagated to Airflow


def process_orders_lists(start_date, end_date):
    try:
        data_inicial, data_final = validate_and_convert_dates(start_date, end_date)
        
        while data_inicial <= data_final:
            start_date, end_date = increment_one_day(data_inicial)
            qs1 = {
                "per_page": 100,
                "f_creationDate": f"creationDate:[{start_date} TO {end_date}]",
            }
            logging.info(f"Processing orders from {start_date} to {end_date}.")
            process_page(qs1)
            data_inicial += timedelta(days=1)

    except Exception as e:
        logging.error(f"An unexpected error occurred while processing orders: {e}")
        raise  # Fail the task in case of any error


def validate_and_convert_dates(start_date, end_date):
    try:
        if not isinstance(start_date, datetime):
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
        if not isinstance(end_date, datetime):
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
        return start_date, end_date
    except ValueError as e:
        logging.error(f"Invalid date format: {e}")
        raise  # Ensure Airflow fails if date conversion fails


def set_globals(api_info, data_conection, coorp_conection, **kwargs):
    global api_conection_info, data_conection_info, coorp_conection_info
    api_conection_info = api_info
    data_conection_info = data_conection
    coorp_conection_info = coorp_conection

    if not all([api_conection_info, data_conection_info, coorp_conection_info]):
        logging.error("Global connection information is incomplete.")
        raise ValueError("All global connection information must be provided.")

    execute_process_orders_list(kwargs["start_date"], kwargs["end_date"])


def execute_process_orders_list(start_date, end_date, delta=None):
    try:
        start_time = time.time()

        if delta:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=delta)

        process_orders_lists(start_date, end_date)
        logging.info("Script executed successfully.")

    except Exception as e:
        logging.error(f"Script encountered an error: {e}")
        raise  # Fail the Airflow task if any exception occurs

    finally:
        logging.info(f"Total execution time: {time.time() - start_time} seconds")

# if __name__ == "__main__":
#     set_globals(
#         {"api_key": "example"}, 
#         {"db_url": "postgresql://user:pass@localhost/db"}, 
#         {"coorp_key": "example"}, 
#         start_date="2024-01-01", 
#         end_date="2024-01-31"
#     )
