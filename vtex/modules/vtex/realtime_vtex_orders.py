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




def get_order_by_id(orderId):
    return make_request(
        api_conection_info["Domain"],
        "GET",
        f"api/oms/pvt/orders/{orderId[0]}",
        params=None,
        headers=api_conection_info["headers"],
    )


def write_orders_to_db(order_id):
    try:
        order_json = get_order_by_id(order_id)

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        raise e

    
    try:
        writer = WriteJsonToPostgres(
                data_conection_info, order_json, "orders", "orderid"
            )
        writer.upsert_data2()
        logging.info("Created record for order ID: %s", order_id)
    except Exception as e:
            logging.error(f"Error creating record - {e}")
            raise e




def process_orders(order_ids):
    """
    Recebe uma LISTA de IDs, ex.: ["1572170578694-01", "1572160578682-01", ...]
    """
    if not order_ids:
        logging.warning("Nenhum order_id recebido para processar.")
        return False

    try:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_order = {
                executor.submit(write_orders_to_db, order_id): order_id
                for order_id in order_ids
            }
            for future in concurrent.futures.as_completed(future_to_order):
                order_id = future_to_order[future]
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Order {order_id} gerou exceção: {e}")
                    raise  # propaga para sinalizar falha geral
        return True
    except Exception as e:
        logging.error(f"Ocorreu um erro inesperado: {e}")
        raise



def get_orders_list_pages(query_params):
    try:
        # print(api_conection_info["Domain"])
        # print(query_params)
        # print(api_conection_info["headers"])
        
        return make_request(
            api_conection_info["Domain"],
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
        tentativa = 0
        resp = None

        # tenta até 5 vezes obter uma resposta válida
        while (resp is None or "list" not in resp) and tentativa < 5:
            resp = get_orders_list_pages(query_params)
            tentativa += 1

        if not resp or "list" not in resp:
            logging.error(f"No orders found for params: {query_params}")
            return []  # retorna lista vazia em vez de None

        orders_list = resp.get("list") or []

        # pega só os orderId
        order_ids = [o.get("orderId") for o in orders_list if o.get("orderId")]
        return order_ids

    except Exception as e:
        logging.exception("Falha ao processar página")
        raise e


def process_orders_lists(start_date, end_date):
    try:
        
        data_inicial, data_final = validate_and_convert_dates(start_date, end_date)
        
        while data_inicial <= data_final:
            start_date, end_date = increment_one_day(data_inicial)
            qs1 = {
                'per_page': 100,
                'f_creationDate': f'creationDate:[{start_date} TO {end_date}]'  # Período desejado
                
            
                # Número de pedidos por página
            }

            logging.info(f"Processing orders from {start_date} to {end_date}.")
            #pega todo os idsorders da lista do dia 
            order_ids=process_page(qs1)
            #processa as orders uma a uma e insere no banco
            process_orders (order_ids)

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
   
   
    # if not all([api_conection_info, data_conection_info, coorp_conection_info]):
    #     logging.error("Global connection information is incomplete.")
    #     raise ValueError("All global connection information must be provided.")

    process_orders_lists(kwargs["start_date"], kwargs["end_date"])


