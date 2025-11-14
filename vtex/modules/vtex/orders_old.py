import concurrent.futures
import logging

from modules.api_conection import make_request
from modules.dbpgconn import WriteJsonToPostgres

api_conection_info = None
data_conection_info = None
coorp_conection_info = None


def get_orders_ids_from_db():
    try:
        if not gb_daily : 
            query = """  	SELECT DISTINCT ora.orderid    
                        FROM orders_list ora   
                        left join orders ord on
                        ora.orderid = ord.orderid
    					where  ord.orderid is null    """
        else:

            query = """   SELECT DISTINCT ora.orderid    
                            FROM orders_list ora      
                            WHERE  is_change = true    """
        result = WriteJsonToPostgres(data_conection_info, query, "orders_list")
        result = result.query()
        return result

    except Exception as e:
        logging.error(f"An unexpected error occurred in get_categories_id_from_db: {e}")
        raise e


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

    

def process_orders():

        # orders_ids = get_orders_ids_from_db()
        # with concurrent.futures.ThreadPoolExecutor() as executor:
        #     executor.map(write_orders_to_db, orders_ids[0])
    try:
        orders_ids = get_orders_ids_from_db()
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            future_to_order = {executor.submit(write_orders_to_db, order_id): order_id for order_id in orders_ids[0]}
            for future in concurrent.futures.as_completed(future_to_order):
                order_id = future_to_order[future]
                try:
                    future.result()  # This will raise an exception if the thread failed
                except Exception as e:
                    logging.error(f"Order {order_id} generated an exception: {e}")
                    raise e  # Raise the exception to ensure task failure
        return True
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        raise e    
        



def set_globals(api_info, data_conection, coorp_conection, isdaily, **kwargs):
    global api_conection_info
    api_conection_info = api_info

    global data_conection_info
    data_conection_info = data_conection

    global coorp_conection_info
    coorp_conection_info = coorp_conection

    global gb_daily
    gb_daily = isdaily


    try:
        process_orders()
    except Exception as e:
        raise e




# if __name__ == "__main__":
#     process_orders()
