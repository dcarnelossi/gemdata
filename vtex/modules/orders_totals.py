from vtex.modules.config import *
from vtex.modules.dbpgconn import *
from vtex.modules.api_conection import *
from vtex.modules.helpers import *

api_conection_info=None
data_conection_info=None
coorp_conection_info=None


def write_orders_totals_to_database_colunar(batch_size=600):
    try:
        
        while True:
            
            query = f"""
            SELECT DISTINCT orders.orderid, orders.totals FROM orders WHERE NOT EXISTS
            (SELECT 1 FROM orders_totals WHERE orders.orderid = orders_totals.orderid) 
            LIMIT {batch_size};"""
            
            result = WriteJsonToPostgres(data_conection_info, query, 'orders_totals').query()
            
            if not result[0]:
                break  # No more results, exit the loop
            
            
            with concurrent.futures.ThreadPoolExecutor() as executor:
                # Mapeia a função para cada item em result usando threads
                results = list(executor.map(process_order_item_colunar, result[0]))

        return True


    except Exception as e:
        logging.error(f"Erro desconhecido - {e}")  
        raise e


def process_order_item_colunar(order_totals):
    try:
        order_id, totals = order_totals
        result_dict = {}

        result = {}
        result["orderid"] = order_id
                
        for item in totals:
            result[item['id']] = item['value']
            
            if 'alternativeTotals' in item:
                for shipins in item['alternativeTotals']:
                    alt_id = shipins['id']
                    alt_value = shipins['value']
                    if alt_id not in result:
                        result[alt_id] = alt_value

        #print(result)     
        writer = WriteJsonToPostgres(data_conection_info, result, 'orders_totals', 'orderid')
        writer.upsert_data()

        logging.info(f"Data upserted successfully for orderid - {result['orderid']}")

        return True, None  # Indica que a execução foi bem-sucedida após o término do loop

    except Exception as e:
        logging.error(f"Erro ao processar item - {e}")  
        raise e


def set_globals(api_info, data_conection,coorp_conection, **kwargs):

    global api_conection_info
    api_conection_info = api_info
    
    global data_conection_info
    data_conection_info = data_conection
    
    global coorp_conection_info
    coorp_conection_info = coorp_conection
    
    write_orders_totals_to_database_colunar()


if __name__ == "__main__":
    write_orders_totals_to_database_colunar()
