import concurrent.futures
import logging

from api_conection import make_request
from dbpgconn import WriteJsonToPostgres

# set Globals
api_conection_info = None
data_conection_info = None


def get_categories_id_from_db():
    try:
        query = """SELECT id, name FROM categories;"""
        result = WriteJsonToPostgres(data_conection_info, query, "categories")
        result = result.query()
        return result

    except Exception as e:
        logging.error(f"An unexpected error occurred in get_categories_id_from_db: {e}")
        raise e


def extract_product_ids(product_list):
    data_list = [item for sublist in product_list["data"].values() for item in sublist]
    return data_list


def get_products_by_category(category_id):
    try:
        query_params = {"categoryId": category_id}
        data = make_request(
            api_conection_info["VTEX_Domain"],
            "GET",
            "api/catalog_system/pvt/products/GetProductAndSkuIds?",
            params=query_params,
            headers=api_conection_info["headers"],
        )
        return extract_product_ids(data)

    except Exception as e:
        logging.error(f"An unexpected error occurred in get_products_by_category: {e}")
        raise e


def get_product_by_id(product_id):
    return make_request(
        api_conection_info["VTEX_Domain"],
        "GET",
        f"api/catalog/pvt/product/{product_id}",
        headers=api_conection_info["headers"],
    )


def process_product(product_id):
    try:
        product_data = get_product_by_id(product_id)
        writer = WriteJsonToPostgres(data_conection_info, product_data, "products", "id")
        writer.upsert_data()
        logging.info("Data inserted successfully.")
        return True

    except Exception as e:
        logging.error(f"Error inserting data into the database in process_product: {e}")
        raise e


def process_products():
    try:
        categories_id = get_categories_id_from_db()

        with concurrent.futures.ThreadPoolExecutor() as executor:
            for category_id in categories_id:
                products_in_category = get_products_by_category(category_id)
                executor.map(process_product, products_in_category)

    except Exception as e:
        logging.error(f"An unexpected error occurred in process_products: {e}")
        raise e


def set_globals(api_info, conection_info):
    global api_conection_info
    api_conection_info = api_info
    global data_conection_info
    data_conection_info = conection_info

    process_products()


if __name__ == "__main__":
    process_products()
