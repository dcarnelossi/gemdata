from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import time

from modules.api_conection import make_request
from modules.dbpgconn import WriteJsonToPostgres




# Variáveis globais
api_conection_info = None
data_conection_info = None
coorp_conection_info = None



def get_orders_list_pages(order_id):
    try:
        return make_request(
            api_conection_info["Domain"],
            "GET",
            f"admin/api/2024-10/orders/{order_id}.json?fields=note_attributes",
            params=None,
            headers = api_conection_info["headers"],
            json=None
           
        )
    except Exception as e:
        logging.error(f"Failed to retrieve orders list pages: {e}")
        raise  # Rethrow the exception to signal the Airflow task failure


def process_order_batch(order_list):
    try:
        writer = WriteJsonToPostgres(
            data_conection_info,
            order_list,
            'shopify_orders',
            'orderid'
        )
        writer.upsert_data_batch(isdatainsercao=1)
        logging.info(f"{len(order_list)} pedidos upsertados com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao upsertar lote de pedidos: {e}")
        raise

def fetch_orders_list(order_id):
   

    order_batch = []


    #preciso fazer um loop caso tenha algum problema, para tentar novamente  
    try:
        response = get_orders_list_pages(order_id)


    except Exception as e:
        logging.error(f"Erro ao pegar o pedido com ID:{order_id}  - Erro: {str(e)}")

    try:
        orders_data = response.get('order', {}).get('note_attributes', [])

        instituicao = next(
        (item.get('value') for item in orders_data if item.get('name') == 'instituicao_doacao'),
        None)
        
        order_batch.append({'orderid':order_id,'instituicao':instituicao})
        
        logging.info(f"Trazendo a isntituicao doacao do id: {order_id}")
        
    except Exception as e:
        logging.error(f"Erro ao processar pedidos - Erro: {str(e)}")
        raise
             
   
    return order_batch  

def validate_and_convert_dates(start_date, end_date):
    try:
        if not isinstance(start_date, datetime):
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
        if not isinstance(end_date, datetime):
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
        return start_date, end_date
    except ValueError as e:
        logging.error(f"Invalid date format: {e}")
        raise

def execute_process_orders(start_date, end_date, minimum_date, delta=None):
    try:
        start_time = time.time()
        if delta:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=delta)
        process_orders_lists(start_date, end_date)
        logging.info("Processamento concluído com sucesso.")
    except Exception as e:
        logging.error(f"Erro durante a execução: {e}")
        raise
    finally:
        logging.info(f"Tempo total de execução: {time.time() - start_time:.2f} segundos.")


def get_orders_ids_from_db(start_date):
    for attempt in range(1, 6):  # Máximo de 5 tentativas
        try:
            query = f"""    
                    select so.orderid  
                    from shopify_orders so
                    where updatedat >= '{start_date}' 
                    order by updatedat asc;
                """
         
            result = WriteJsonToPostgres(data_conection_info, query, "shopify_orders")
            result = result.query()
            return result  # Sucesso: retorna os resultados

        except Exception as e:
            logging.warning(f"[Tentativa {attempt}/5] Erro ao buscar orders: {e}")
            time.sleep(2)  # Espera antes de tentar novamente

            if attempt == 5:
                logging.error("Falha após 5 tentativas ao buscar orders no banco.")
                raise e  # Lança a exceção final se todas as tentativas falharem


def process_orders_lists(start_date, end_date):
    try:
        # data_inicial, data_final = validate_and_convert_dates(start_date, end_date)
                      
        BATCH_SIZE = 500
        # i = 0 

        list_orders_id =  get_orders_ids_from_db(start_date)
        
        #print(6119302299747)
        #print(list_orders_id)
        
        if not list_orders_id[0]:
            logging.info("Nenhum item para ser processado")
            return

        all_orders_batch = []
        

        def fetch_with_retries(order_id):
           
            for attempt in range(1, 6):
                try:
                
                    return fetch_orders_list(order_id)
                
                except Exception as e:
                    logging.warning(f"Tentativa {attempt}/5 falhou para o pedido {order_id}: {e}")
                    time.sleep(2)
            logging.error(f"Pedido {order_id} falhou após 5 tentativas.")
            return []

        with ThreadPoolExecutor(max_workers=100) as executor:
            futures = [
                executor.submit(fetch_with_retries, order_id[0])
                for order_id in list_orders_id[0]
            ]

            for future in as_completed(futures):
                try:
                    batch = future.result()
                    all_orders_batch.extend(batch)
                    
             
                    while len(all_orders_batch) >= BATCH_SIZE:
                        process_order_batch(all_orders_batch[:BATCH_SIZE])
                        all_orders_batch = all_orders_batch[BATCH_SIZE:]
                except Exception as e:
                    logging.error(f"Erro ao processar batch de items em paralelo: {e}")

        if all_orders_batch:
            process_order_batch(all_orders_batch)

    except Exception as e:
        logging.error(f"Erro inesperado ao processar pedidos: {e}")
        raise

    


def set_globals(api_info, data_conection, coorp_conection,start_date,end_date,minimum_date):
    global api_conection_info
    api_conection_info = api_info

    global data_conection_info
    data_conection_info = data_conection

    global coorp_conection_info
    coorp_conection_info = coorp_conection

  
    try:
        execute_process_orders(start_date,end_date,minimum_date)
    except Exception as e:
        raise e




