import concurrent.futures
import http.client
import json
import logging
from urllib.parse import urlencode

from dbpgconn import WriteJsonToPostgres


def get_categories_tree(category_levels):
    try:
        conn = http.client.HTTPSConnection(VTEX_Domain)

        conn.request(
            "GET",
            f"/api/catalog_system/pub/category/tree/{category_levels}",
            headers=headers,
        )

        res = conn.getresponse()
        data = res.read()

        if res.status == 200:
            print(data.decode("utf-8"))
            # save_json_to_blob_storage("categories",data.decode("utf-8"))
            for category_id in extract_category_ids(data.decode("utf-8")):
                # get_products_by_category(id)
                result, error_ids = get_products_by_category(category_id)
                if error_ids:
                    logging.error(
                        f"Error fetching data for the following product IDs: {error_ids}"
                    )
            return result
        else:
            logging.error(f"Error: {res.status} - {res.reason}")
            return None

    except http.client.HTTPException as http_error:
        logging.error(f"HTTPException: {http_error}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return None
    finally:
        conn.close()


def get_product_variations(product_id):
    try:
        conn = http.client.HTTPSConnection(VTEX_Domain)

        conn.request(
            "GET",
            f"/api/catalog_system/pub/products/variations/{product_id}",
            headers=headers,
        )

        res = conn.getresponse()
        data = res.read()

        if res.status == 200:
            return data.decode("utf-8")
        else:
            logging.error(f"Error: {res.status} - {res.reason}")
            return None

    except http.client.HTTPException as http_error:
        logging.error(f"HTTPException: {http_error}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return None
    finally:
        conn.close()


def extract_category_ids(category_list):
    # Carregar a string JSON
    dados_json = json.loads(category_list)

    # Função para extrair recursivamente os IDs
    def extrair_ids(objeto):
        ids = [objeto["id"]]
        if "children" in objeto:
            for child in objeto["children"]:
                ids.extend(extrair_ids(child))
        return ids

    # Extrair os IDs usando a função recursiva
    category_ids = []
    for item in dados_json:
        category_ids.extend(extrair_ids(item))

    print(category_ids)
    return category_ids


# Função para percorrer a estrutura JSON e extrair os IDs
def extract_product_ids(product_list):
    # Carregar a string JSON
    dados_json = json.loads(product_list)
    # Extrair os primeiros objetos do campo "data" para uma lista
    data_list = [item for sublist in dados_json["data"].values() for item in sublist]
    print(data_list)
    return data_list


# Chamada da função para preencher a lista
# extract_product_ids(json_data)


def check_and_create_table(category_data):
    product_ids = extract_product_ids(category_data)
    first_product_id = next(iter(product_ids), None)

    if first_product_id is not None:
        try:
            product_data = json.loads(get_request_by_id("product", first_product_id))
        except Exception as e:
            logging.error(f"Error fetching product data: {e}")
            return None

        # Check if the table exists
        writer = WriteJsonToPostgres(product_data, "products")
        if not writer.table_exists():
            try:
                writer.create_table()
                logging.error("criou tabela -----------------------------------------")
            except Exception as e:
                logging.error(f"Erro desconhecido - {e}")

    return product_data if "product_data" in locals() else None


def get_request_by_id(endpoint, id):
    try:
        conn = http.client.HTTPSConnection(VTEX_Domain)
        conn.request("GET", f"/api/catalog/pvt/{endpoint}/{id}", headers=headers)
        res = conn.getresponse()
        data = res.read()

        if res.status == 200:
            return data.decode("utf-8")
        else:
            logging.error(f"Error: {res.status} - {res.reason}")
            return None

    except http.client.HTTPException as http_error:
        logging.error(f"HTTPException: {http_error}")
        return None

    except Exception as e:
        logging.error(f"get_request_by_id - {id} - An unexpected error occurred: {e}")
        return None

    finally:
        conn.close()


def process_product_data(product_id, error_ids):
    try:
        product_data = get_request_by_id("product", product_id)

        if product_data is not None:
            # Continue processing the product data and inserting into the database
            writer = WriteJsonToPostgres(json.loads(product_data), "products")

            try:
                writer.insert_data()
                logging.error("criou registro -----------------------------------------")
            except Exception as e:
                logging.error(f"Error inserting data into the database: {e}")
                error_ids.append(product_id)
        else:
            # Handle the case where get_request_by_id returned None
            logging.error(f"Error fetching data for product ID {product_id}")
            error_ids.append(product_id)

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        error_ids.append(product_id)


def get_products_by_category(category_id):
    try:
        conn = http.client.HTTPSConnection(VTEX_Domain)

        # Construa a string de consulta usando urlencode
        query_params = urlencode({"categoryId": category_id})

        conn = http.client.HTTPSConnection(VTEX_Domain)
        conn.request(
            "GET",
            f"/api/catalog_system/pvt/products/GetProductAndSkuIds?{query_params}",
            headers=headers,
        )
        res = conn.getresponse()
        data = res.read()

        if res.status == 200:
            # Call the separate function to check and create the table
            product_data = check_and_create_table(data.decode("utf-8"))

            if product_data is not None:
                # Continue processing other products with ThreadPoolExecutor
                error_ids = []
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    executor.map(
                        lambda product_id: process_product_data(product_id, error_ids),
                        extract_product_ids(data.decode("utf-8")),
                    )

                # Log error IDs to the file
                if error_ids:
                    logging.info(
                        f"Error fetching data for the following product IDs: {error_ids}"
                    )

                return data.decode("utf-8"), error_ids

            else:
                logging.warning("No product data found.")
                return None, None
        else:
            logging.error(f"Error: {res.status} - {res.reason}")
            return None, None

    except http.client.HTTPException as http_error:
        logging.error(f"HTTPException: {http_error}")
        return None, None
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return None, None
    finally:
        conn.close()


def normalise_products_by_id(product_id):
    try:
        json_product = json.loads(get_request_by_id("product", product_id))

        category_id = json_product["CategoryId"]
        print(category_id)
        json_category_str = get_request_by_id("category", category_id)

        # Verifica se json_category_str não é None e pode ser convertido para um objeto JSON
        if json_category_str:
            try:
                json_category = json.loads(json_category_str)
                json_product["Category"] = json_category
            except json.JSONDecodeError as e:
                # Faça algo se a conversão falhar, como lançar uma exceção ou definir um valor padrão
                json_product["Category"] = {}
                logging.error(f"Error decoding JSON for category {category_id}: {e}")
        else:
            # Faça algo se json_category_str for None, como lançar uma exceção ou definir um valor padrão
            json_product["Category"] = {}

        brand_id = json_product["BrandId"]
        print(brand_id)
        json_brand_str = get_request_by_id("brand", brand_id)

        # Verifica se json_brand_str não é None e pode ser convertido para um objeto JSON
        if json_brand_str:
            try:
                json_brand = json.loads(json_brand_str)
                json_product["Brand"] = json_brand
            except json.JSONDecodeError as e:
                # Faça algo se a conversão falhar, como lançar uma exceção ou definir um valor padrão
                json_product["Brand"] = {}
                logging.error(f"Error decoding JSON for brand {brand_id}: {e}")
        else:
            # Faça algo se json_brand_str for None, como lançar uma exceção ou definir um valor padrão
            json_product["Brand"] = {}

        if json_product["IsActive"]:
            json_variations_str = get_product_variations(product_id)

            # Verifica se json_variations_str não é None e pode ser convertido para um objeto JSON
            if json_variations_str:
                try:
                    json_variations = json.loads(json_variations_str)
                    json_product["Variations"] = json_variations
                except json.JSONDecodeError as e:
                    # Faça algo se a conversão falhar, como lançar uma exceção ou definir um valor padrão
                    json_product["Variations"] = {}
                    logging.error(
                        f"Error decoding JSON for variations of product {product_id}: {e}"
                    )
            else:
                # Faça algo se json_variations_str for None, como lançar uma exceção ou definir um valor padrão
                json_product["Variations"] = {}

        logging.info(json.dumps(json_product))

        return json.dumps(json_product)
    except Exception as e:
        logging.error(
            f"get_product_id - {product_id} - An unexpected error occurred: {e}"
        )
        return None


if __name__ == "__main__":
    # get_products_by_category(61)

    # normalise_products_by_id(19)

    get_categories_tree(30)

# get_product_variations(19)
