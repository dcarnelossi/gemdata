import concurrent.futures
import logging

import requests

# from api_conection import make_request
from modules.dbpgconn import WriteJsonToPostgres

# set Globals
api_conection_info = None
data_conection_info = None

session = requests.Session()


def make_request(method, path, params=None):
    try:
        response = session.request(
            method,
            f"https://{api_conection_info['VTEX_Domain']}/api/catalog_system/pvt/sku/{path}",
            params=params,
            headers=api_conection_info["headers"],
        )
        response.raise_for_status()
        # print (response.json())
        return response.json() if response.status_code == 200 else None
    except requests.RequestException as e:
        logging.error(f"Request failed: {e}")
        return None


def get_skus_list_pages(page):
    query_params = {"page": page, "pagesize": 1000}
    return make_request("GET", "stockkeepingunitids", params=query_params)


def get_skus_ids(init_page):
    skus_ids = []

    while True:
        skus_page = get_skus_list_pages(init_page)

        if not skus_page:
            break

        skus_ids.extend(skus_page)
        init_page += 1

    # logging.info(skus_ids)
    return skus_ids


def get_sku_by_id(sku_id):
    return make_request("GET", f"stockkeepingunitbyid/{sku_id}")


def process_sku(sku_id):
    sku_json = get_sku_by_id(sku_id)

    if sku_json:
        # logging.info(sku_id)

        # if not writer.table_exists():
        #     try:
        #         writer.create_table()
        #         logging.info("Created table -----------------------------------------")
        #     except Exception as e:
        #         logging.error(f"Unknown error - {e}")

        try:
            # decoded_data = json.loads(sku_json)
            writer = WriteJsonToPostgres(data_conection_info, sku_json, "skus", "Id")
            writer.upsert_data()
            logging.info("Data upsert successfully.")
            return True
        except Exception as e:
            logging.error(f"Error inserting data - {e}")
            return False
    else:
        logging.error(f"Error inserting data - {sku_json}")
        return False


def get_skus(init_page):
    try:
        skus = get_skus_ids(init_page)

        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(process_sku, skus)

        return True

    except Exception as e:
        logging.error(f"get_skus - An unexpected error occurred: {e}")
        return None


def set_globals(init_page, api_info, conection_info):
    global api_conection_info
    api_conection_info = api_info
    global data_conection_info
    data_conection_info = conection_info

    get_skus(init_page)


if __name__ == "__main__":
    get_skus(1)
