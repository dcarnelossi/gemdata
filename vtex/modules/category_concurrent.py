import concurrent.futures
import json
import logging

import requests
from modules.dbpgconn import WriteJsonToPostgres

category_levels = None
api_conection_info = None
data_conection_info = None


def log_error(msg, exception=None):
    logging.error(f"{msg}: {exception}" if exception else msg)


# TODO Trocar isso aqui pra função que esta no api_conection
def make_request(method, endpoint, params=None):
    try:
        with requests.Session() as session:
            response = session.request(
                method,
                f"https://{api_conection_info['Domain']}{endpoint}",
                headers=api_conection_info["headers"],
                params=params,
            )
            # Gera exceção se a resposta indicar erro (status HTTP >= 400)
            response.raise_for_status()
            return response.json()

    except requests.RequestException as e:
        log_error(f"Error in HTTP request to {endpoint}", e)
        raise
    except json.JSONDecodeError as e:
        log_error(f"JSON decoding error for response from {endpoint}", e)
        raise
    except Exception as e:
        log_error(f"An unexpected error occurred in HTTP request to {endpoint}", e)
        raise

    return None


def handle_category_data(category_id, data):
    try:
        decoded_data = json.loads(data)
        writer = WriteJsonToPostgres(
            data_conection_info, decoded_data, "categories", "Id"
        )

        # if not writer.table_exists():
        #     try:
        #         writer.create_table()
        #         logging.info("Table created successfully.")
        #     except Exception as e:
        #         log_error(f"Error creating table - {e}")

        writer.upsert_data2()
        logging.info("Data upsert_data successfully.")

    except json.JSONDecodeError as e:
        log_error(f"JSON decoding error: {e}")
        raise
    except Exception as e:
        log_error(
            f"An unexpected error occurred in handle_category_data - {category_id}: {e}"
        )
        raise



def extract_category_ids(objeto):
    ids = [objeto["id"]]
    if "children" in objeto:
        for child in objeto["children"]:
            ids.extend(extract_category_ids(child))
    return ids


def extract_category_ids_wrapper(category_list):
    try:
        dados_json = json.loads(category_list)
        category_ids = [id for item in dados_json for id in extract_category_ids(item)]
        logging.info(f"Extracted category IDs: {category_ids}")
        return category_ids

    except json.JSONDecodeError as e:
        log_error(f"JSON decoding error in extract_category_ids: {e}")
        raise
    except Exception as e:
        log_error(f"An unexpected error occurred in extract_category_ids: {e}")
        raise

   


def process_category_id(category_id):
    
    try:
        category_details = make_request("GET", f"/api/catalog/pvt/category/{category_id}")

        if category_details:
            logging.info(f"Processing completed for category {category_id}.")
            handle_category_data(category_id, json.dumps(category_details))
            return category_details

        logging.error(f"Processing failed for category {category_id}.")
        return None
    except Exception as e:
        log_error(f"process_category_id : {e}")
        raise


def fetch_categories_from_api(category_levels):
    try:
        return make_request(
            "GET", f"/api/catalog_system/pub/category/tree/{category_levels}"
        )
    except Exception as e:
        log_error(f"Error fetching categories from API: {e}")
        raise


def process_categories(data):
    try:
        category_lists = extract_category_ids_wrapper(json.dumps(data))

        # with concurrent.futures.ThreadPoolExecutor() as executor:
        #     list(executor.map(process_category_id, category_lists))
        
        with concurrent.futures.ThreadPoolExecutor() as executor:
                    future_to_category = {
                        executor.submit(process_category_id, category_l): category_l 
                        for category_l in category_lists
                    }
                    # Itera conforme as tarefas forem completadas
                    for future in concurrent.futures.as_completed(future_to_category):
                        category_l = future_to_category[future]
                        try:
                            result = future.result()  # Lança exceção se houver falha na tarefa
                            logging.info(f"category ID {category_l} processado com sucesso.")
                        except Exception as e:
                            logging.error(f"category ID {category_l} gerou uma exceção: {e}")
                            raise e  # Lança a exceção para garantir que o erro seja capturado
               


    except Exception as e:
        log_error(f"Error processing categories: {e}")


def set_globals(categories_info, api_info, conection_info):
    global category_levels
    category_levels = categories_info
    global api_conection_info
    api_conection_info = api_info
    global data_conection_info
    data_conection_info = conection_info

    process_category_tree(categories_info)


def process_category_tree(category_levels):
    try:
        data = fetch_categories_from_api(category_levels)

        if data:
            logging.info("Categories tree retrieved successfully.")
            process_categories(data)

        return data
    except Exception as e:
        log_error(f"Error processing category tree: {e}")
        raise e


if __name__ == "__main__":
    # Exemplo de uso
    result = process_category_tree(30)  # Substitua 3 pelo número desejado de níveis
    if result:
        logging.info("Processing completed.")
    else:
        logging.error("Processing failed.")
