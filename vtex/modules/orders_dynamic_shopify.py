from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import time
import requests

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



def load_graphql_query(query_type):
    for attempt in range(1, 6):
        try:
            query_modificado = f"""
                SELECT parameter FROM public.integrations_parameter_api
                WHERE id=  '{data_conection_info['schema']}' 
                ORDER BY date_modification DESC
                LIMIT 1;
            """
            
            result = WriteJsonToPostgres(coorp_conection_info, query_modificado, "integrations_parameter_api")
            result, _ = result.query()
            if not result or not result[0]:
                query_default = f"""
                    SELECT parameter FROM public.integrations_parameter_api
                    WHERE name=  'default' 
                    ORDER BY date_modification DESC
                    LIMIT 1;
                """
                result = WriteJsonToPostgres(coorp_conection_info, query_default, "integrations_parameter_api")
                result, _ = result.query()
            data = result[0][0]
            if query_type not in data["shopify"]:
                raise ValueError(f"Query '{query_type}' não encontrada no JSON")
            return data["shopify"][query_type]
        except Exception as e:
            logging.warning(f"[Tentativa {attempt}/5] Erro ao buscar orders: {e}")
            time.sleep(2)
            if attempt == 5:
                logging.error("Falha após 5 tentativas ao buscar orders no banco.")
                raise e

def safe_get(dictionary, path, default=None):
    keys = path.split(".")
    value = dictionary
    try:
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        return value if value not in ["", None, {}] else default
    except (KeyError, TypeError, AttributeError):
        return default

def transform_shopify_response(data, structure, data_path, order_id=None):
    transformed_data = {"list": []}
    nodes = safe_get(data, data_path, [])
    for item in nodes:
        node = item.get("node", item)
        transformed_item = {}
        context = {"node": node, "order_id": order_id, "safe_get": safe_get}
        for key, path in structure.items():
            try:
                transformed_item[key] = eval(path, context) if node else None
            except Exception:
                transformed_item[key] = None
        transformed_data["list"].append(transformed_item)
    return transformed_data

def substituir_orders_list(query, start_date="", end_date="", minimum_date="", order_id=""):
    try:
        substituicoes = {
            "{start_date}": start_date,
            "{end_date}": end_date,
            "{minimum_date}": minimum_date,
            "{order_id}": str(order_id),
        }
        for chave, valor in substituicoes.items():
            query = query.replace(chave, valor)
        return query
    except Exception as e:
        logging.error(f"Erro ao substituir os argumento da query{query} - Erro: {str(e)}")
        raise

def process_order_batch(order_list, table, keytable):
    for attempt in range(1, 6):  # Máximo de 5 tentativas
        try:
            writer = WriteJsonToPostgres(
                data_conection_info,
                order_list,
                table,
                keytable
            )
            writer.upsert_data_batch(isdatainsercao=1)
            
        
        except Exception as e:
                logging.warning(f"[Tentativa {attempt}/5] Erro ao upsertar lote de pedidos: {e}")
                time.sleep(30)
                if attempt == 5:
                    logging.error("Falha após 5 tentativas ao upsertar orders no banco.")
                    raise e
    

def fetch_orders_list(json_type_api, start_date, end_date, minimum_date, order_id=""):
   
    cursor = None
    has_next_page = True
    graphql_query_parameters = json_type_api["graphql_query"].strip()
    structure = json_type_api["structure"]
    data_path = json_type_api["data_path"]
    
    query = substituir_orders_list(graphql_query_parameters, start_date, end_date, minimum_date, order_id)


    order_batch = []

    while has_next_page:
        pagination = f', after: "{cursor}"' if cursor else ""
        graphql_query = query.replace("{pagination}", pagination)
        
        #preciso fazer um loop caso tenha algum problema, para tentar novamente  
        try:
            response = get_orders_list_pages(graphql_query)

           
        except Exception as e:
            logging.error(f"Erro ao pegar o pedido com ID:{graphql_query}  - Erro: {str(e)}")
            raise


        try:
            
            
            
            orders_data = response.get("data", {}).get("orders", {}) or response.get("data", {}).get("order", {})

            if not orders_data:
                logging.info("orders_data está vazio. Pulando para a próxima iteração.")
                break  # ou `break`, se quiser sair do loop

            orders_list = transform_shopify_response(response, structure, data_path, order_id)
            
            print(orders_list)
            for order in orders_list["list"]:
                order_batch.append(order)

            print(order_batch) 
            
            has_next_page = orders_data.get('pageInfo', {}).get('hasNextPage', False)
            cursor = orders_data.get('pageInfo', {}).get('endCursor', None)
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

def execute_process_orders(query_type, start_date, end_date, minimum_date, delta=None):
    try:
        start_time = time.time()
        if delta:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=delta)
        process_orders_lists(query_type, start_date, end_date, minimum_date)
        logging.info("Processamento concluído com sucesso.")
    except Exception as e:
        logging.error(f"Erro durante a execução: {e}")
        raise
    finally:
        logging.info(f"Tempo total de execução: {time.time() - start_time:.2f} segundos.")


def get_orders_ids_from_db(query_type, start_date):
    for attempt in range(1, 6):  # Máximo de 5 tentativas
        try:
            if query_type == "items":
                # query = f"""    
                #     select o.orderid from "96481bf3-87fe-4150-b136-ede2550acc57".shopify_orders_items i
                #     right join "96481bf3-87fe-4150-b136-ede2550acc57".shopify_orders o on 
                #     o.orderid = i.orderid
                #     where i.orderid is null   
                # """
                
                query = f"""    
                    select so.orderid  
                    from shopify_orders so
                    where updatedat >= '{start_date}' 
                    order by updatedat asc;
                """
            else:
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

# def remove_duplicates_by_orderid(order_list):
#     unique_orders = {}
#     for order in order_list:
#         order_id = order.get("orderid")
#         # Always overwrite, so it keeps the last occurrence
#         unique_orders[order_id] = order
#     return list(unique_orders.values())



def process_orders_lists(query_type, start_date, end_date, minimum_date):
    try:
        data_inicial, data_final = validate_and_convert_dates(start_date, end_date)
        data_min, _ = validate_and_convert_dates(minimum_date, end_date)
        min_date = data_min.replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        logging.info(f"Data mínima: {minimum_date}")
        json_type_api = load_graphql_query(query_type)

        table = json_type_api["tablepg"]
        keytable = json_type_api["keytablepg"]
        BATCH_SIZE = 100

        if query_type == "orders":
            
            def fetch_range_with_retries(start_d, end_d):
                for attempt in range(1, 6):
                    try:
                        logging.info(f"Processando período {start_d} a {end_d}")
                        return fetch_orders_list(json_type_api, start_d, end_d, min_date)
                    except Exception as e:
                        logging.warning(f"Tentativa {attempt}/5 falhou em puxar o dado api (fetch_orders_list)  para o período {start_d} a {end_d}: {e}")
                        time.sleep(60)
                logging.error(f"Falha após 5 tentativas - dado api (fetch_orders_list)-  no período {start_d} a {end_d}")
                raise Exception(f"Falha definitiva ao buscar pedido -dado api (fetch_orders_list)- {start_d} a {end_d}")
            
            def insert_orders_batch(all_orders_batch,start_d,end_d):
                for attempt in range(1, 6):    
                    try:
                        logging.info(f"Inserindo o batch {start_d} a {end_d}")
                        #remove_duplicates_by_orderid(all_orders_batch)
                        return   process_order_batch(
                                    all_orders_batch,
                                    table,
                                    keytable
                                )
                        
                    except Exception as e:
                        logging.warning(f"Tentativa {attempt}/5 falhou na insercao do postgree para o período {start_d} a {end_d}: {e}")
                        time.sleep(2)
                
                logging.error(f"Falha após 5 tentativas na insercao no postgree no período {start_d} a {end_d}")
                raise Exception(f"Falha definitiva insercao no postgree ao buscar pedido {start_d} a {end_d}")
        
            # Gera todas as faixas de data a serem processadas
            while data_inicial < data_final:
                start_d, end_d = increment_one_day(data_inicial)
                data_inicial += timedelta(days=1)
          
                all_orders_batch = []
                logging.info(f"Processando período {start_d} a {end_d}")
                all_orders_batch= fetch_range_with_retries(start_d, end_d)
                insert_orders_batch(all_orders_batch,start_d, end_d)


        else:  # query_type == "items"
            list_orders_id = get_orders_ids_from_db(query_type, start_date)
            if not list_orders_id[0]:
                logging.info("Nenhum item para ser processado")
                return

            all_orders_batch = []

            def fetch_with_retries(order_id):
                for attempt in range(1, 6):
                    try:
                        # logging.info(f"Processando orders items ou payment - {order_id}")
                        return fetch_orders_list(json_type_api, start_date, start_date, start_date, order_id)
                    except Exception as e:
                        logging.warning(f"Tentativa {attempt}/5 falhou para o pedido {order_id}: {e}")
                        time.sleep(60)
                logging.error(f"Pedido {order_id} falhou após 5 tentativas.")
                raise Exception(f"Falha definitiva ao buscar pedido {order_id}")

            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = [
                    executor.submit(fetch_with_retries, order_id[0])
                    for order_id in list_orders_id[0]
                ]

                for future in as_completed(futures):
                    try:
                        batch = future.result()
                        if batch: 
                            all_orders_batch.extend(batch)

                            while len(all_orders_batch) >= BATCH_SIZE:
                                all_orders_batch=retirar_duplicata_id(all_orders_batch)    
                                process_order_batch(
                                    all_orders_batch[:BATCH_SIZE],
                                    table,
                                    keytable
                                )
                                all_orders_batch = all_orders_batch[BATCH_SIZE:]

                    except Exception as e:
                        logging.error(f"Erro ao processar batch de items em paralelo: {e}")
                        raise
                         
            if all_orders_batch:
                all_orders_batch=retirar_duplicata_id(all_orders_batch)  
                process_order_batch(
                    all_orders_batch,
                    table,
                    keytable
                )

    except Exception as e:
        logging.error(f"Erro inesperado ao processar pedidos: {e}")
        raise

    
def retirar_duplicata_id(batch):
    vistos = set()
    resultado = []
    for item in batch:
        id_item = item['orderid'] if isinstance(item, dict) else item[0]
        if id_item not in vistos:
            resultado.append(item)
            vistos.add(id_item)
    return resultado

def set_globals(api_info, data_conection, coorp_conection,start_date,end_date,minimum_date,type_api,**kwargs):
    global api_conection_info
    api_conection_info = api_info

    global data_conection_info
    data_conection_info = data_conection

    global coorp_conection_info
    coorp_conection_info = coorp_conection

  
    try:
        execute_process_orders(type_api,start_date,end_date,minimum_date)
    except Exception as e:
        raise e




