from vtex.modules.config import *
from vtex.modules.dbpgconn import *


def get_brands_list(api_conection_info):
    try:
        conn = http.client.HTTPSConnection(api_conection_info['VTEX_Domain'])

        conn.request("GET", f"/api/catalog_system/pvt/brand/list", headers=api_conection_info['headers'])

        res = conn.getresponse()
        data = res.read()

        if res.status == 200:
            print(data.decode("utf-8"))
            #save_json_to_blob_storage("brands","brands",data.decode("utf-8"))
            for id in extract_brand_ids(data.decode("utf-8")):
                get_brand_id(id)
            return data.decode("utf-8")
        else:
            print(f"Error: {res.status} - {res.reason}")
            return None

    except http.client.HTTPException as http_error:
        print(f"HTTPException: {http_error}")
        return None
    except Exception as e:
        print(f"get_brands_list - An unexpected error occurred: {e}")
        return None
    finally:
        conn.close()
        
        
def get_brands_list_parallel(api_conection_info, data_conection_info):
    try:
        
        print("brand")
              
        conn = http.client.HTTPSConnection(api_conection_info['VTEX_Domain'])

        conn.request("GET", f"/api/catalog_system/pvt/brand/list", headers=api_conection_info['headers'])

        res = conn.getresponse()
        data = res.read()
        
        print("data")
        print(data)

        if res.status == 200:
            logging.info(data.decode("utf-8"))
            # save_json_to_blob_storage("brands","brands",data.decode("utf-8"))

            # Use ThreadPoolExecutor para obter dados de marcas em paralelo
            brand_ids = extract_brand_ids(data.decode("utf-8"))
            with concurrent.futures.ThreadPoolExecutor() as executor:
                #executor.map(get_brand_id, brand_ids, api_conection_info, data_conection_info)
                executor.map(lambda brand_id: get_brand_id(api_conection_info, data_conection_info, brand_id), brand_ids)

            return True
        else:
            logging.error(f"Error: {res.status} - {res.reason}")
            return None

    except http.client.HTTPException as http_error:
        logging.error(f"HTTPException: {http_error}")
        return None
    except Exception as e:
        logging.error(f"get_brands_list_parallel - An unexpected error occurred: {e}")
        return None
    finally:
        conn.close()


def get_brand_id(api_conection_info, data_conection_info, brand_id):
    try:
        conn = http.client.HTTPSConnection(api_conection_info['VTEX_Domain'])

        conn.request("GET", f"/api/catalog/pvt/brand/{brand_id}", headers=api_conection_info['headers'])

        res = conn.getresponse()
        data = res.read()

        if res.status == 200:
            # save_json_to_blob_storage("brands",f"brand_{brand_id}", data.decode("utf-8"))
            # save_json_to_cosmosdb("brands", data.decode("utf-8"))

            writer = WriteJsonToPostgres(data_conection_info, json.loads(data.decode("utf-8")), 'brands','Id')

            # if not writer.table_exists():
            #     try:
            #         writer.create_table()
            #         logging.info("Table created successfully.")
            #     except Exception as e:
            #         logging.error(f"Error creating table - {e}")

            try:
                #writer.insert_data()
                writer.upsert_data()
                logging.info("Data upsert successfully.")
                return True
            except Exception as e:
                logging.error(f"Error inserting data - {e}")
                return False

            # return data.decode("utf-8")
        else:
            logging.error(f"Error: {res.status} - {res.reason}")
            return None
    except http.client.HTTPException as http_error:
        logging.error(f"HTTPException: {http_error}")
        return None
    except Exception as e:
        logging.error(f"get_brand_id - {brand_id} - An unexpected error occurred: {e}")
        return None
    finally:
        conn.close()


# Função para percorrer a estrutura JSON e extrair os IDs
def extract_brand_ids(brand_list):
    # Carregar a string JSON
    dados_json = json.loads(brand_list)

    # Função para extrair recursivamente os IDs
    def extrair_ids(objeto):
        ids = [objeto['id']]
        if 'children' in objeto:
            for child in objeto['children']:
                ids.extend(extrair_ids(child))
        return ids

    # Extrair os IDs usando a função recursiva
    brand_ids = []
    for item in dados_json:
        brand_ids.extend(extrair_ids(item))

    print(brand_ids)
    return brand_ids


if __name__ == "__main__":

    get_brands_list_parallel()