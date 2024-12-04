import concurrent.futures
import logging
import time
from datetime import datetime, timedelta
import concurrent.futures
import logging
import time



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


def get_orders_count_query(start_date, end_date,minimum_date):
    return f"""
    {{
      ordersCount(query: "updated_at:>='{start_date}' AND updated_at:<'{end_date}' AND created_at:>='{minimum_date}'") {{
        count
      }}
    }}
    """

# Função para construir a consulta GraphQL
def get_orders_query(start_date, end_date,minimum_date, cursor=None):
    pagination = f', after: "{cursor}"' if cursor else ""
    return f"""
    {{
      orders(first: 100{pagination}, query: "updated_at:>='{start_date}' AND updated_at:<'{end_date}' AND created_at:>='{minimum_date}'") {{
        edges {{
          cursor
          node {{
            id
            name
            email
            createdAt
            updatedAt
            closedAt
            cancelledAt
            cancelReason
            currencyCode
            currentSubtotalPriceSet {{
              shopMoney {{
                amount
                currencyCode
              }}
            }}
            currentTotalDiscountsSet {{
              shopMoney {{
                amount
                currencyCode
              }}
            }}
            currentTotalPriceSet {{
              shopMoney {{
                amount
                currencyCode
              }}
            }}
            currentTotalTaxSet {{
              shopMoney {{
                amount
                currencyCode
              }}
            }}
            customerLocale
            displayFinancialStatus
            totalWeight
            totalShippingPriceSet {{
              shopMoney {{
                amount
                currencyCode
              }}
            }}
            totalTaxSet {{
              shopMoney {{
                amount
                currencyCode
              }}
            }}
            totalPriceSet {{
              shopMoney {{
                amount
                currencyCode
              }}
            }}
            shippingAddress {{
              firstName
              lastName
              company
              city
              zip
              provinceCode
              countryCode
            }}
            billingAddress {{
              company
              address1
              address2
              city
              zip
              provinceCode
              countryCode
            }}
            channelInformation {{
              channelDefinition {{
                channelName
                subChannelName
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
    """


def orders_count_from_db(start_date, end_date,minimum_date):
    try:
        query = f"""     select count(1) from shopify_orders so 
                        WHERE  data_insercao >='{start_date}' and  data_insercao <='{end_date}'
     
                           """
        result = WriteJsonToPostgres(data_conection_info, query, "shopify_orders")
        result = result.query()
        return result

    except Exception as e:
        logging.error(f"An unexpected error occurred in get_categories_id_from_db: {e}")
        raise e




def verificar_contagem(start_date, end_date,minimum_date,qtdorders,dataini_upsert, datafim_upsert):

    try:
        loopcount = 0
        
     
        while loopcount<5 :
            count, _ = orders_count_from_db(dataini_upsert, datafim_upsert,minimum_date)
            countorderpg = count[0][0]
            query = get_orders_count_query(start_date, end_date,minimum_date)
            

            
            #responsecount = requests.post(url, headers=headers, json={"query": query})
            responsecount =  get_orders_list_pages(query) 
        
          
            total_count_shopify = responsecount.get("data", {}).get("ordersCount", {}).get("count")

            logging.info(F"VERIFICANDO SE TEM TODAS AS ORDERS {qtdorders} -QTD BD : {countorderpg} = {total_count_shopify} QTD SHOPIFY")
            
            if total_count_shopify ==qtdorders and  countorderpg == qtdorders:
              return 1
            else:
              return 0
              
            
    except Exception as e:
      logging.error(f"Erro ao processar pedidos: {e}")
      raise e

def process_order(order):
    try:    
        # Salvar dados no PostgreSQL ou em arquivo
       
        writer = WriteJsonToPostgres(
                data_conection_info, 
                order,  # Encapsular os dados em um objeto JSON
                "shopify_orders", 
                "orderid"
            )
        # else:
        #     writer = WriteJsonToPostgres(data_conection_info, orders_data, "orders_list_daily", "id")

        writer.upsert_data(isdatainsercao=1)
        logging.info(f"Order {order['orderid']} upserted successfully.")
    
    except Exception as e:
        logging.error(f"Error inserting order {order['orderid']}: {e}")
        raise  # Ensure failure is propagated to Airflow



def fetch_orders_list(start_date, end_date,minimum_date):
    orders = []
    cursor = None
    has_next_page = True

    while has_next_page:
        query = get_orders_query(start_date, end_date,minimum_date, cursor)
        response =  get_orders_list_pages(query) 
        #print(response)
        #requests.post(url, headers=headers, json={'query': query})
        
        
        try: 
           
            data = response.get('data', {}).get('orders', {})
            edges = data.get('edges', [])
            
            # Normaliza os dados para JSON
            for edge in edges:
                order_data = edge.get('node', {})
                # Transformar order_data para alinhar com as colunas do banco
                orderid={order_data.get("id")}
                transformed_order = {
                            "orderid": int(order_data.get("id").split("/")[-1]),
                            "idshopify": order_data.get("id"),
                            "name": order_data.get("name", None),
                            "email": order_data.get("email", None),
                            "createdat": order_data.get("createdAt", None),
                            "updatedat": order_data.get("updatedAt", None),
                            "closedat": order_data.get("closedAt", None),
                            "cancelledat": order_data.get("cancelledAt", None),
                            "cancelreason": order_data.get("cancelReason", None),
                            "currencycode": order_data.get("currencyCode", None),
                            "currentsubtotalprice": order_data.get("currentSubtotalPriceSet", {}).get("shopMoney", {}).get("amount", None),
                            "currenttotaldiscounts": order_data.get("currentTotalDiscountsSet", {}).get("shopMoney", {}).get("amount", None),
                            "currenttotalprice": order_data.get("currentTotalPriceSet", {}).get("shopMoney", {}).get("amount", None),
                            "currenttotaltax": order_data.get("currentTotalTaxSet", {}).get("shopMoney", {}).get("amount", None),
                            "customerlocale": order_data.get("customerLocale", None),
                            "displayfinancialstatus": order_data.get("displayFinancialStatus", None),
                            "totalweight": int(order_data.get("totalWeight", 0)) if order_data.get("totalWeight") is not None else None,
                            "totalshippingprice": order_data.get("totalShippingPriceSet", {}).get("shopMoney", {}).get("amount", None),
                            "totaltax": order_data.get("totalTaxSet", {}).get("shopMoney", {}).get("amount", None),
                            "totalprice": order_data.get("totalPriceSet", {}).get("shopMoney", {}).get("amount", None),
                            "shippingfirstname": order_data.get("shippingAddress", {}).get("firstName", None),
                            "shippingcity": order_data.get("shippingAddress", {}).get("city", None),
                            "shippingzip": order_data.get("shippingAddress", {}).get("zip", None),
                            "shippingprovincecode": order_data.get("shippingAddress", {}).get("provinceCode", None),
                            "shippingcountrycode": order_data.get("shippingAddress", {}).get("countryCode", None),
                            "billingcity": order_data.get("billingAddress", {}).get("city", None) if order_data.get("billingAddress") else None,
                            "billingzip": order_data.get("billingAddress", {}).get("zip", None) if order_data.get("billingAddress") else None,
                            "billingprovincecode": order_data.get("billingAddress", {}).get("provinceCode", None) if order_data.get("billingAddress") else None,
                            "billingcountrycode": order_data.get("billingAddress", {}).get("countryCode", None) if order_data.get("billingAddress") else None,
                            "channelname": order_data.get("channelInformation", {}).get("channelDefinition", {}).get("channelName", None) if order_data.get("channelInformation", {}) else None,
                            "subchannelname": order_data.get("channelInformation", {}).get("channelDefinition", {}).get("subChannelName", None) if order_data.get("channelInformation", {}) else None,
                        }
                orders.append(transformed_order)
            
            # Controle de paginação
            has_next_page = data.get('pageInfo', {}).get('hasNextPage', False)
            cursor = data.get('pageInfo', {}).get('endCursor', None)
        except Exception as e:
            logging.error(f"Erro ao pegar o pedido com ID: {orderid} - Erro: {str(e)}")
            raise


        
   #print({"list": orders})
    # Retorna a lista de pedidos no formato esperado
    return {"list": orders}

def process_orders_and_save(start_date, end_date,minimum_date):
    
    countloop = 0  # Número máximo de tentativas
    while  countloop < 4 :
        try:
            if(countloop==3):
                time.sleep(30)
            # Reduz o contador de tentativas a cada iteração
            orders_data = fetch_orders_list(start_date, end_date,minimum_date)
            dataini_upsert = datetime.now()  
            if not orders_data:
                logging.info("Nenhum pedido encontrado.")
                return

            orders_list = orders_data["list"]  # Obtém a lista de pedidos
            
            if not orders_list:
                logging.info("Nenhum pedido encontrado na lista.")
                return

            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                futures = {executor.submit(process_order, order): order for order in orders_list}
                for future in concurrent.futures.as_completed(futures):
                    order = futures[future]
                    try:
                        future.result()
                    except Exception as e:
                        logging.error(f"Erro ao processar pedido {order.get('orderId')}: {e}")
                        raise  # Propaga a exceção para falhar a tarefa do Airflow
                    #time.sleep(5)     
            datafim_upsert = datetime.now()        
            # Sucesso no processamento, reseta contador de tentativas
            veri=verificar_contagem(start_date, end_date,minimum_date,len(orders_list),dataini_upsert,datafim_upsert)

            if veri==0: 
              countloop = countloop +1
            else:
              countloop =5

        except Exception as e:
            logging.error(f"Erro ao processar pedidos: {e}")
            countloop = countloop +1   # Reduz o número de tentativas restantes

    if countloop == 4:
      logging.error("Limite de tentativas alcançado. Interrompendo a execução.")
      raise Exception(f"Erro ao processar pedidos após {4} tentativas. Intervalo: {start_date} a {end_date} ")  
      



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

def process_orders_lists(start_date, end_date,minimum_date):
    try:
        data_inicial, data_final = validate_and_convert_dates(start_date, end_date)
        min_date = (minimum_date.replace(hour=00, minute=00, second=00, microsecond=000000)).strftime(
        "%Y-%m-%dT%H:%M:%S.%fZ")
        print(f"o minimo da data é :{minimum_date}")
        while data_inicial < data_final:

            start_date, end_date = increment_one_day(data_inicial)

            
           
            logging.info(f"Processing orders from {start_date} to {end_date}.")
            process_orders_and_save(start_date, end_date,min_date)
            data_inicial += timedelta(days=1)

    except Exception as e:
        logging.error(f"An unexpected error occurred while processing orders: {e}")
        raise  # Fail the task in case of any error

# Função principal
def execute_process_orders(start_date, end_date,minimum_date, delta=None):
    try:
        start_time = time.time()

        if delta:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=delta)


        process_orders_lists(start_date, end_date,minimum_date)
        logging.info("Processamento concluído com sucesso.")
    except Exception as e:
        logging.error(f"Erro durante a execução: {e}")
        raise
    finally:
        logging.info(f"Tempo total de execução: {time.time() - start_time:.2f} segundos.")




# execute_process_orders('2024-01-01','2024-01-02')


def set_globals(api_info, data_conection, coorp_conection,start_date,end_date,minimum_date,**kwargs):
    global api_conection_info
    api_conection_info = api_info

    global data_conection_info
    data_conection_info = data_conection

    global coorp_conection_info
    coorp_conection_info = coorp_conection

    #print(minimum_date)

  
    try:
        execute_process_orders(start_date,end_date, minimum_date)
    except Exception as e:
        raise e
