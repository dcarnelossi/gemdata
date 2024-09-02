import concurrent.futures
import logging

from modules.api_conection import make_request
from modules.dbpgconn import WriteJsonToPostgres

# Variáveis globais
api_conection_info = None
data_conection_info = None

def get_categories_id_from_db():
    try:
        query = """select distinct cast(productid as int)  from skus;"""
        result = WriteJsonToPostgres(data_conection_info, query, "skus").query()
        if not result:
            logging.warning("No skus found in the database.")
        return result
    except Exception as e:
        logging.error(f"An error occurred in get_categories_id_from_db: {e}")
        raise  # Ensure the Airflow task fails on error

def get_product_by_id(product_id):
    try:
        return make_request(
            api_conection_info["VTEX_Domain"],
            "GET",
            f"api/catalog/pvt/product/{product_id}",
            headers=api_conection_info["headers"],
        )
    except Exception as e:
        logging.error(f"An error occurred in get_product_by_id for product_id {product_id}: {e}")
        raise  # Ensure the Airflow task fails on error
    
def process_product(product_id):
    try:
        product_data = get_product_by_id(product_id)
        writer = WriteJsonToPostgres(data_conection_info, product_data, "products", "id")
        writer.upsert_data()
        logging.info(f"Product data inserted successfully for product_id {product_id}.")
    except Exception as e:
        logging.error(f"Error inserting data for product_id {product_id} in process_product: {e}")
        raise  # Ensure the Airflow task fails on error

def process_products():
    try:
        categories_id = get_categories_id_from_db()
        if not categories_id:
            logging.warning("No categories to process.")
            return

        with concurrent.futures.ThreadPoolExecutor() as executor:
            for category_id in categories_id:
                print(category_id[0])
                executor.map(process_product, category_id[0])
             #   else:
             #       logging.warning(f"No products found for category_id {category_id}.")

    except Exception as e:
        logging.error(f"An error occurred in process_products: {e}")
        raise  # Ensure the Airflow task fails on error

def set_globals(api_info, conection_info):
    global api_conection_info, data_conection_info
    api_conection_info = api_info
    data_conection_info = conection_info

    if not all([api_conection_info, data_conection_info]):
        logging.error("Global connection information is incomplete.")
        raise ValueError("All global connection information must be provided.")

    process_products()

# if __name__ == "__main__":
#     set_globals(
#         {"api_key": "example"}, 
#         {"db_url": "postgresql://user:pass@localhost/db"}
#     )
