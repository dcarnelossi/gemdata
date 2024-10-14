import concurrent.futures
import logging
import requests
from modules.dbpgconn import WriteJsonToPostgres

# set Globals
api_conection_info = None
data_conection_info = None

session = requests.Session()

def make_request(method, path, params=None):
    if not api_conection_info:
        logging.error("API connection info is not set.")
        raise ValueError("API connection info is not set.")
    
    try:
        response = session.request(
            method,
            f"https://{api_conection_info['VTEX_Domain']}/api/catalog_system/pvt/sku/{path}",
            params=params,
            headers=api_conection_info["headers"],
        )
        response.raise_for_status()
        return response.json() if response.status_code == 200 else None
    except requests.JSONDecodeError as e:
        logging.error(f"Failed to parse JSON response: {e}")
        raise
    except requests.RequestException as e:
        logging.error(f"Request failed: {e}")
        raise

def get_skus_list_pages(page):
    query_params = {"page": page, "pagesize": 1000}
    return make_request("GET", "stockkeepingunitids", params=query_params)

def get_skus_ids(init_page):
    
    try:
    
        skus_ids = []
        while True:
            skus_page = get_skus_list_pages(init_page)
            if not skus_page:
                logging.info(f"No more SKUs found starting from page {init_page}.")
                break
            skus_ids.extend(skus_page)
            init_page += 1
        return skus_ids
    except Exception as e:
        logging.error(f"get_skus - An unexpected error occurred: {e}")
        raise 

def get_sku_by_id(sku_id):
    return make_request("GET", f"stockkeepingunitbyid/{sku_id}")

class SkuNotFoundException(Exception):
    pass

def process_sku(sku_id):
    sku_json = get_sku_by_id(sku_id)
    if sku_json:
        try:
            writer = WriteJsonToPostgres(data_conection_info, sku_json, "skus", "Id")
            writer.upsert_data2()
            logging.info(f"SKU {sku_id} upserted successfully.")
            return True
        except Exception as e:
            logging.error(f"Error inserting SKU {sku_id} data - {e}")
            return e
    else:
        logging.error(f"SKU not found for ID: {sku_id}")
        raise SkuNotFoundException(f"SKU with ID {sku_id} not found.")

def get_skus(init_page):
    if not api_conection_info or not data_conection_info:
        logging.error("Global connection info is not set.")
        raise ValueError("Global connection info is not set.")
    
    try:
        skus = get_skus_ids(init_page)
        if not skus:
            logging.info("No SKUs found to process.")
            return False

        # with concurrent.futures.ThreadPoolExecutor() as executor:
        #     futures = {executor.submit(process_sku, sku): sku for sku in skus}
        #     for future in concurrent.futures.as_completed(futures):
        #         sku = futures[future]
        #         try:
        #             future.result()
        #         except Exception as e:
        #             logging.error(f"Error processing SKU {sku}: {e}")
        # return True
    
        with concurrent.futures.ThreadPoolExecutor() as executor:
                    future_to_sku = {
                        executor.submit(process_sku,sku ): sku 
                        for sku in skus
                    }
                    # Itera conforme as tarefas forem completadas
                    for future in concurrent.futures.as_completed(future_to_sku):
                        sku = future_to_sku[future]
                        try:
                            result = future.result()  # Lança exceção se houver falha na tarefa
                            logging.info(f"sku {sku} processado com sucesso.")
                        except Exception as e:
                            logging.error(f"sku {sku} gerou uma exceção: {e}")
                            raise e  # Lança a exceção para garantir que o erro seja capturado
        return True

    except Exception as e:
        logging.error(f"get_skus - An unexpected error occurred: {e}")
        raise e

def set_globals(init_page, api_info, conection_info):
    global api_conection_info
    api_conection_info = api_info
    global data_conection_info
    data_conection_info = conection_info

    get_skus(init_page)

# if __name__ == "__main__":
#     set_globals(1, {"VTEX_Domain": "example.com", "headers": {"Authorization": "Bearer your_token"}}, {"db_info": "details"})
