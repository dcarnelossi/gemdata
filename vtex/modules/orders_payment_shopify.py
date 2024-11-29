import concurrent.futures
import logging
import time
from datetime import datetime, timedelta
import concurrent.futures
import logging


from modules.api_conection import make_request
from modules.dbpgconn import WriteJsonToPostgres
from modules.helpers import increment_one_day



# Variáveis globais
api_conection_info = None
data_conection_info = None
coorp_conection_info = None
isdaily = None


def get_order_transactions_query(order_id):
    return f"""
    {{
      order(id: "gid://shopify/Order/{order_id}") {{
            id
            transactions {{
                id
                kind
                paymentMethod
                status
                amountSet {{
                    shopMoney {{
                        amount
                        currencyCode
                    }}
                }}
                gateway
                createdAt
            }}
      }}
    }}
    """


def get_orders_list_pages(query_params):
    try:
        return make_request(
            api_conection_info["Domain"],
            "POST",
            "admin/api/2024-10/graphql.json",
            params=None,
            headers = api_conection_info["headers"],
            json={'query': query_params}
            #headers=api_conection_info["headers"],
           
        )
    except Exception as e:
        logging.error(f"Failed to retrieve orders list pages: {e}")
        raise  # Rethrow the exception to signal the Airflow task failure


    

def fetch_order_transactions_list(order_id):
    """
    Busca as transações de um pedido específico e transforma os dados para inserção na tabela orders_payment.
    """
    transactions = []

    # Construir a consulta para as transações do pedido
    query = get_order_transactions_query(order_id)
    response = get_orders_list_pages(query) 

    try:
            # Extrai os dados do GraphQL
            response_data = response
            data = response_data.get('data', {}).get('order', {})
            #print(response.json())
            if not data:
                logging.warning(f"Nenhum dado encontrado para o pedido {order_id}")
                return {"list": []}

            transaction_list = data.get('transactions', [])
            if not transaction_list:
                logging.info(f"Nenhuma transação encontrada para o pedido {order_id}")
                return {"list": []}

            # Processar os dados das transações
            for transaction_data in transaction_list:
                # Transformar transaction_data para alinhar com as colunas do banco
                transformed_transaction = {
                    "orderid": int(order_id),
                    "idpaymentshopify": transaction_data.get("id"),
                    "transactionid": transaction_data.get("id"),
                    "kind": transaction_data.get("kind"),
                    "paymentMethod": transaction_data.get("paymentMethod"),                    
                    "status": transaction_data.get("status"),
                    "amount": transaction_data.get("amountSet", {}).get("shopMoney", {}).get("amount"),
                    "currencycode": transaction_data.get("amountSet", {}).get("shopMoney", {}).get("currencyCode"),
                    "gateway": transaction_data.get("gateway"),
                    "createdat": transaction_data.get("createdAt"),
                }
                transactions.append(transformed_transaction)

    except Exception as e:
            logging.error(f"Erro ao processar transações do pedido {order_id}: {str(e)}")
            raise
    
    # Retorna a lista de transações no formato esperado
    return {"list": transactions}


def get_orders_ids_from_db(start_date=None):
    try:
        if start_date is None:
            query = f"""    
                select  so.orderid
                FROM shopify_orders so
                LEFT JOIN shopify_orders_payment oi
                ON  oi.orderid = so.orderid
                where oi.orderid is null 
                """ 
        else:

             #  print(start_date)
            query = f"""    
                select so.orderid  
                from shopify_orders so
                where updatedat >= '{start_date}' ;
                """
            
        
        result = WriteJsonToPostgres(data_conection_info, query, "shopify_orders_payment")
        result = result.query()
        return result

    except Exception as e:
        logging.error(f"An unexpected error occurred in get_categories_id_from_db: {e}")
        raise e


# Função para processar e salvar os dados no banco
def process_orders(start_date):
    try:
        orders_ids = get_orders_ids_from_db(start_date)
       # print(orders_ids)

        if not orders_ids[0]:
                logging.info("Nenhum item para ser processado")
                return

        veri=[]
        countloop = 0  # Número máximo de tentativas
        while  countloop < 4 :
        #    print(orders_ids)
        #    print(veri)
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                future_to_order = {executor.submit(process_order_item, order_id[0]): order_id for order_id in orders_ids[0]}
                for future in concurrent.futures.as_completed(future_to_order):
                    order_id = future_to_order[future]
                    try:
                        future.result()  # This will raise an exception if the thread failed
                    except Exception as e:
                        logging.error(f"Order {order_id} generated an exception: {e}")
                        raise e  # Raise the exception to ensure task failure
            
            # Sucesso no processamento, reseta contador de tentativas
            veri=get_orders_ids_from_db(start_date=None)

            if veri[0]: 
              countloop = countloop +1
              orders_ids=veri
              erroid=veri[0]
              time.sleep(60)
            else:
              countloop =10
              return 
            
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        raise e    
    
    if countloop == 4:
      logging.error(f"Limite de tentativas alcançado. Interrompendo a execução.{erroid}")
      raise Exception(f"Erro ao processar pedidos após {5} tentativas. Intervalos ")      
        
# Função para processar e salvar os dados no banco
def process_order_item(order_id):
    try:
        #print(order_id)
        orders_item = fetch_order_transactions_list(order_id)
        if not orders_item:
            logging.info("Nenhum pedido encontrado.")
            return
      #  print(orders_item)
        orders_item_list = orders_item["list"]  # Obtém a lista de pedidos
        
      #  print(orders_item_list)
        if not orders_item_list:
            logging.info("Nenhum pedido encontrado na lista.")
            return

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            futures = {executor.submit(process_order_item_save, order): order for order in orders_item_list}
            for future in concurrent.futures.as_completed(futures):
                order = futures[future]
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Erro ao processar pedido {order.get('orderId')}: {e}")
                    raise  # Propaga a exceção para falhar a tarefa do Airflow

    except Exception as e:
        logging.error(f"Erro ao processar pedidos: {e}")
        raise



def process_order_item_save(order):
    try:    
        # Salvar dados no PostgreSQL ou em arquivo
        if not isdaily:
            writer = WriteJsonToPostgres(
                data_conection_info, 
                order,  # Encapsular os dados em um objeto JSON
                "shopify_orders_payment", 
                "idpaymentshopify"
            )
        # else:
        #     writer = WriteJsonToPostgres(data_conection_info, orders_data, "orders_list_daily", "id")

        writer.upsert_data(isdatainsercao=1)
        logging.info(f"Order {order['idpaymentshopify']} upserted successfully.")
    
    except Exception as e:
        logging.error(f"Error inserting order {order['idpaymentshopify']}: {e}")
        raise  # Ensure failure is propagated to Airflow

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
# Função principal
def execute_process_orders(data_inicial):
    try:
        start_time = time.time()

        data_inicial, _=validate_and_convert_dates(data_inicial,data_inicial)    
        start_date,_ = increment_one_day(data_inicial)

        process_orders(start_date)
        logging.info("Processamento concluído com sucesso.")
    except Exception as e:
        logging.error(f"Erro durante a execução: {e}")
        raise
    finally:
        logging.info(f"Tempo total de execução: {time.time() - start_time:.2f} segundos.")


def set_globals(api_info, data_conection, coorp_conection,start_date,**kwargs):
    global api_conection_info
    api_conection_info = api_info

    global data_conection_info
    data_conection_info = data_conection

    global coorp_conection_info
    coorp_conection_info = coorp_conection

  
    try:
        execute_process_orders(start_date)
    except Exception as e:
        raise e
