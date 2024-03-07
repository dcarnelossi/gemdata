from vtex.modules.config import *
from vtex.modules.dbpgconn import *
from vtex.modules.api_conection import *
from vtex.modules.helpers import *

api_conection_info=None
data_conection_info=None
coorp_conection_info=None


def write_orders_shippingdata_to_database(batch_size=600):
    try:
        while True:
            
            query = f"""SELECT orders.orderid, orders.shippingdata
                        FROM orders
                        WHERE orders.orderid NOT IN (SELECT orderid FROM orders_shippingdata WHERE orderid IS NOT NULL)
                        ORDER BY orders.sequence
                        LIMIT {batch_size};"""

            result = WriteJsonToPostgres(data_conection_info, query, 'orders_shippingdata')
            result = result.query()

            if not result[0]:
                break  # No more results, exit the loop

            with concurrent.futures.ThreadPoolExecutor() as executor:
                # Mapeia a função para cada item em result usando threads
                results = list(executor.map(process_orders_shippingdata, result[0]))
                
        return True
    
    except Exception as e:
        logging.error(f"write_orders_shippingdata_to_database - Erro desconhecido - {e}")
        raise e


def process_orders_shippingdata(result):
    try:
        order_id, orders_shippingdata = result

        json = {
            "selectedAddresses": orders_shippingdata.get("selectedAddresses", []),
            "address": orders_shippingdata.get("address", {}),
            "orderid": order_id
        }
                
        new_json=flatten_json(json)

        writer = WriteJsonToPostgres(data_conection_info, new_json, 'orders_shippingdata', 'orderid')
        writer.upsert_data()
        
        logging.info(f"Inserção de dados concluída para orderid - {order_id}")

        return True, None  # Indica que a execução foi bem-sucedida

    except Exception as e:
        error_message = f"Erro ao processar orders_shippingdata - {e}"
        logging.error(error_message)
        print(error_message)
        return False, str(e)  # Indica que houve uma falha na execução e fornece detalhes do erro
    
    
def set_globals(api_info, data_conection,coorp_conection, **kwargs):

    global api_conection_info
    api_conection_info = api_info
    
    global data_conection_info
    data_conection_info = data_conection
    
    global coorp_conection_info
    coorp_conection_info = coorp_conection
    
    write_orders_shippingdata_to_database()
    

if __name__ == "__main__":
    write_orders_shippingdata_to_database()
