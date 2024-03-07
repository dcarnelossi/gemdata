from vtex.modules.config import *
from vtex.modules.dbpgconn import *
from vtex.modules.api_conection import *
from vtex.modules.helpers import *


api_conection_info=None
data_conection_info=None
coorp_conection_info=None


def write_client_profile_to_database(batch_size=600):
    try:
        
        while True:
            # Query dinâmica
            query = f"""SELECT DISTINCT orders.orderid, orders.clientprofiledata 
                        FROM orders 
                        WHERE NOT EXISTS (SELECT 1 FROM client_profile WHERE orders.orderid = client_profile.orderid )
                        LIMIT {batch_size};"""
            result = WriteJsonToPostgres(data_conection_info, query, 'client_profile').query()
            
            if not result[0]:
                break  # No more results, exit the loop

            with concurrent.futures.ThreadPoolExecutor() as executor:
                # Mapeia a função para cada item em result usando threads
                results = list(executor.map(process_client_profile, result[0]))
                
        return True

    except Exception as e:
        logging.error(f"write_client_profile_to_database - Erro desconhecido - {e}")
        raise e


def process_client_profile(result):
    try:
        order_id, client_profile = result
        client_profile['orderid'] = order_id

        writer = WriteJsonToPostgres(data_conection_info, client_profile, 'client_profile', 'orderid')
        writer.upsert_data()
        logging.info(f"Inserção de dados concluída para orderid - {order_id}")

        return True, None  # Indica que a execução foi bem-sucedida

    except Exception as e:
        error_message = f"Erro ao processar item - {e}"
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
    
    write_client_profile_to_database()


if __name__ == "__main__":
    write_client_profile_to_database()