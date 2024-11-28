import concurrent.futures
import logging
import time
from datetime import datetime
import concurrent.futures
import logging


from modules.api_conection import make_request
from modules.dbpgconn import WriteJsonToPostgres
from modules.helpers import increment_one_day_shopify


# Variáveis globais
api_conection_info = None
data_conection_info = None
coorp_conection_info = None
isdaily = None




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


# Função para construir a consulta GraphQL para itens do pedido
def get_order_line_items_query(order_id, cursor=None):
    pagination = f', after: "{cursor}"' if cursor else ""
    return f"""
    {{
      order(id: "gid://shopify/Order/{order_id}") {{
        id
        lineItems(first: 100{pagination}) {{
          edges {{
            node {{
              id
              title
              quantity
              originalUnitPriceSet {{
                shopMoney {{
                  amount
                  currencyCode
                }}
              }}
              variant {{
                id
                title
                sku
                price
                product {{
                  id
                  title
                  vendor
                  productType
                }}
              }}
            discountAllocations {{
                allocatedAmountSet {{
                    shopMoney {{
                        amount
                        currencyCode
                    }}
                }}
                }}
            }}
          }}
          pageInfo {{
            hasNextPage
            endCursor
          }}
        }}
      }}
    }}
    """




def fetch_order_items_list(order_id):
    """
    Busca os itens de um pedido específico e transforma os dados para inserção na tabela shopify_orders_items.
    """
    items = []
    cursor = None
    has_next_page = True

    while has_next_page:
        # Construir a consulta para os itens do pedido
            query = get_order_line_items_query(order_id, cursor)
            response = get_orders_list_pages(query) 
        
       
            # Extrai os dados do GraphQL
            data = response.get('data', {}).get('order', {})
            
            if not data:
                logging.warning(f"Nenhum dado encontrado para o pedido {order_id}")
                break

            edges = data.get('lineItems', {}).get('edges', [])
            
            # Processar os dados dos itens
            for edge in edges:
                item_data = edge.get('node', {})
                if not item_data:
                    continue


                # Verificar se 'variant' e 'product' estão presentes
                variant_data = item_data.get("variant") or {}
                product_data = variant_data.get("product") or {}

                total_discount_amount = 0.0
                for allocation in item_data.get("discountAllocations", []):
                    discount_amount = allocation.get("allocatedAmountSet", {}).get("shopMoney", {}).get("amount")
                    if discount_amount:
                        total_discount_amount += float(discount_amount)

                # Transformar item_data para alinhar com as colunas do banco
                transformed_item = {
                    "orderid": int(order_id),  # Extrai o ID numérico do pedido
                    "iditemshopify": item_data.get("id"),
                    "title": item_data.get("title"),
                    "quantity": item_data.get("quantity"),
                    "originalunitprice": item_data.get("originalUnitPriceSet", {}).get("shopMoney", {}).get("amount"),
                    "originalunitcurrencycode": item_data.get("originalUnitPriceSet", {}).get("shopMoney", {}).get("currencyCode"),
                    "variantid": variant_data.get("id"),
                    "varianttitle": variant_data.get("title"),
                    "variantsku": variant_data.get("sku"),
                    "variantprice": variant_data.get("price"),
                    "productid": product_data.get("id"),
                    "producttitle": product_data.get("title"),
                    "productvendor": product_data.get("vendor"),
                    "producttype": product_data.get("productType"),
                    "totaldiscountamount": total_discount_amount

                }
                items.append(transformed_item)

            # Controle de paginação
            has_next_page = data.get('lineItems', {}).get('pageInfo', {}).get('hasNextPage', False)
            cursor = data.get('lineItems', {}).get('pageInfo', {}).get('endCursor', None)
        

    # Retorna a lista de itens no formato esperado
    return {"list": items}


def get_orders_ids_from_db(start_date=None):
    try:
        if start_date:
          #  print(start_date)
            query = f"""    
            select so.orderid  
            from shopify_orders so
            where updatedat >= '{start_date}' ;
            """
        else:
            query = f"""    
                select  so.orderid
                FROM shopify_orders so
                LEFT JOIN shopify_orders_items oi
                ON  oi.orderid = so.orderid
                where cancelreason is null and so.currentsubtotalprice <>0
                GROUP BY so.orderid
                HAVING max(so.currentsubtotalprice +so.currenttotaldiscounts  ) <> COALESCE(SUM(oi.originalunitprice*oi.quantity),0.00);  
                """ 
        
        result = WriteJsonToPostgres(data_conection_info, query, "shopify_orders" )
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

            if not veri: 
              countloop = countloop +1
              orders_ids=veri
            else:
              countloop =10
              return 
            
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        raise e    
    
    if countloop == 4:
      logging.error("Limite de tentativas alcançado. Interrompendo a execução.")
      raise Exception(f"Erro ao processar pedidos após {5} tentativas. Intervalos ")      
        
# Função para processar e salvar os dados no banco
def process_order_item(order_id):
    try:
        #print(order_id)
        orders_item = fetch_order_items_list(order_id)

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
                "shopify_orders_items", 
                "iditemshopify"
            )
        # else:
        #     writer = WriteJsonToPostgres(data_conection_info, orders_data, "orders_list_daily", "id")

        writer.upsert_data(isdatainsercao=1)
        logging.info(f"Order{order['orderid']} - item: {order['iditemshopify']} upserted successfully.")
    
    except Exception as e:
        logging.error(f"Error inserting order {order['iditemshopify']}: {e}")
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
        start_date,_ = increment_one_day_shopify(data_inicial)

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
