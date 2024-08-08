import concurrent.futures
import logging
import time
from datetime import datetime, timedelta

from api_conection import make_request
from dbpgconn import WriteJsonToPostgres
from helpers import increment_one_day

api_conection_info = None
data_conection_info = None
coorp_conection_info = None


def get_orders_list_pages(query_params):
    return make_request(
        api_conection_info["VTEX_Domain"],
        "GET",
        "api/oms/pvt/orders",
        params=query_params,
        headers=api_conection_info["headers"],
    )


def process_page(query_params):
    try:
        print(query_params)
        lista = get_orders_list_pages(query_params)
        list = lista["list"]

        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(process_order, list)

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        raise e


def process_order(order):
    try:
        # decoded_data = json.loads(sku_json)
        writer = WriteJsonToPostgres(
            data_conection_info, order, "orders_list", "orderid"
        )
        writer.upsert_data()
        logging.info("Data upserted successfully.")
        return True
    except Exception as e:
        logging.error(f"Error inserting data - {e}")
        return False


def process_orders_lists(start_date, end_date):
    try:
        data_inicial = start_date
        data_final = end_date

        if not isinstance(data_inicial, datetime):
            try:
                data_inicial = datetime.strptime(data_inicial, "%Y-%m-%d")
            except Exception as e:
                logging.error(f"Não foi possível converter a data - {e}")
                raise e

        logging.info("Data Inicial %s", data_inicial)

        if not isinstance(data_final, datetime):
            try:
                data_final = datetime.strptime(data_final, "%Y-%m-%d")
            except Exception as e:
                logging.error(f"Não foi possível converter a data - {e}")
                raise e

        logging.info("Data Final %s", data_final)

        while data_inicial <= data_final:
            start_date, end_date = increment_one_day(data_inicial)
            qs1 = {
                "per_page": 100,
                "f_creationDate": f"creationDate:[{start_date} TO {end_date}]",
            }
            logging.info(qs1)
            process_page(qs1)
            data_inicial += timedelta(days=1)

        return True

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        raise e


def set_globals(api_info, data_conection, coorp_conection, **kwargs):
    global api_conection_info
    api_conection_info = api_info

    global data_conection_info
    data_conection_info = data_conection

    global coorp_conection_info
    coorp_conection_info = coorp_conection

    execute_process_orders_list(kwargs["start_date"], kwargs["end_date"])


def execute_process_orders_list(start_date, end_date, delta=None):
    try:
        # usado pra log
        start_time = time.time()

        if delta:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=delta)

        result = process_orders_lists(start_date, end_date)
        logging.info("Script executed successfully.")
    except Exception as e:
        logging.error(f"Script encountered an error: {e}")
    finally:
        logging.info(f"Total execution time: {time.time() - start_time} seconds")
