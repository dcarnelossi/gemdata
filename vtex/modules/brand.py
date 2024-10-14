import concurrent.futures
import http.client
import json
import logging

from modules.dbpgconn import WriteJsonToPostgres


def get_brands_list(api_conection_info):
    try:
        conn = http.client.HTTPSConnection(api_conection_info["VTEX_Domain"])

        conn.request(
            "GET",
            "/api/catalog_system/pvt/brand/list",
            headers=api_conection_info["headers"],
        )

        res = conn.getresponse()
        data = res.read()

        if res.status == 200:
            print(data.decode("utf-8"))
            # save_json_to_blob_storage("brands","brands",data.decode("utf-8"))
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

        conn = http.client.HTTPSConnection(api_conection_info["VTEX_Domain"])

        conn.request(
            "GET",
            "/api/catalog_system/pvt/brand/list",
            headers=api_conection_info["headers"],
        )

        res = conn.getresponse()
        data = res.read()

        print("data")
        print(data)

        if res.status == 200:
            logging.info(data.decode("utf-8"))
            # save_json_to_blob_storage("brands","brands",data.decode("utf-8"))

            try:
                # Use ThreadPoolExecutor para obter dados de marcas em paralelo
                brand_ids = extract_brand_ids(data.decode("utf-8"))
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future_to_brand = {
                        executor.submit(get_brand_id, api_conection_info, data_conection_info, brand_id): brand_id 
                        for brand_id in brand_ids
                    }
                    # Itera conforme as tarefas forem completadas
                    for future in concurrent.futures.as_completed(future_to_brand):
                        brand_id = future_to_brand[future]
                        try:
                            result = future.result()  # Lança exceção se houver falha na tarefa
                            logging.info(f"Brand ID {brand_id} processado com sucesso.")
                        except Exception as e:
                            logging.error(f"Brand ID {brand_id} gerou uma exceção: {e}")
                            raise e  # Lança a exceção para garantir que o erro seja capturado
                return True


                # with concurrent.futures.ThreadPoolExecutor() as executor:
                #     # executor.map
                #     # (get_brand_id, brand_ids, api_conection_info, data_conection_info)
                #     executor.map(
                #         lambda brand_id: get_brand_id(
                #             api_conection_info, data_conection_info, brand_id
                #         ),
                #         brand_ids,
                #     )

            except Exception as e:
                logging.error(f"An unexpected error occurred: {e}")
                raise e    
            
            # # Use ThreadPoolExecutor para obter dados de marcas em paralelo
            # brand_ids = extract_brand_ids(data.decode("utf-8"))
            # with concurrent.futures.ThreadPoolExecutor() as executor:
            #     # executor.map
            #     # (get_brand_id, brand_ids, api_conection_info, data_conection_info)
            #     executor.map(
            #         lambda brand_id: get_brand_id(
            #             api_conection_info, data_conection_info, brand_id
            #         ),
            #         brand_ids,
            #     )

            # return True
        else:
            logging.error(f"Error: {res.status} - {res.reason}")
            return None

    except http.client.HTTPException as http_error:
        logging.error(f"HTTPException: {http_error}")
        raise 
    except Exception as e:
        logging.error(f"get_brands_list_parallel - An unexpected error occurred: {e}")
        raise
    finally:
        conn.close()


def get_brand_id(api_conection_info, data_conection_info, brand_id):
    try:
        conn = http.client.HTTPSConnection(api_conection_info["VTEX_Domain"])

        conn.request(
            "GET",
            f"/api/catalog/pvt/brand/{brand_id}",
            headers=api_conection_info["headers"],
        )

        res = conn.getresponse()
        data = res.read()

        if res.status == 200:
            # save_json_to_blob_storage("brands",f"brand_{brand_id}",
            # data.decode("utf-8"))
            # save_json_to_cosmosdb("brands", data.decode("utf-8"))

            writer = WriteJsonToPostgres(
                data_conection_info, json.loads(data.decode("utf-8")), "brands", "Id"
            )

            # if not writer.table_exists():
            #     try:
            #         writer.create_table()
            #         logging.info("Table created successfully.")
            #     except Exception as e:
            #         logging.error(f"Error creating table - {e}")

            try:
                # writer.insert_data()
                writer.upsert_data2()
                logging.info("Data upsert successfully.")
                return True
            except Exception as e:
                logging.error(f"Error inserting data - {e}")
                raise

            # return data.decode("utf-8")
        else:
            logging.error(f"Error: {res.status} - {res.reason}")
            return None
    except http.client.HTTPException as http_error:
        logging.error(f"HTTPException: {http_error}")
        raise
    except Exception as e:
        logging.error(f"get_brand_id - {brand_id} - An unexpected error occurred: {e}")
        raise
    finally:
        conn.close()


# Função para percorrer a estrutura JSON e extrair os IDs
def extract_brand_ids(brand_list):
    
    try:

        # Carregar a string JSON
        dados_json = json.loads(brand_list)

        # Função para extrair recursivamente os IDs
        def extrair_ids(objeto):
            ids = [objeto["id"]]
            if "children" in objeto:
                for child in objeto["children"]:
                    ids.extend(extrair_ids(child))
            return ids

        # Extrair os IDs usando a função recursiva
        brand_ids = []
        for item in dados_json:
            brand_ids.extend(extrair_ids(item))

        print(brand_ids)
        return brand_ids
    except Exception as e:
        logging.error(f"extract_brand_ids - An unexpected error occurred: {e}")
        raise

if __name__ == "__main__":
    get_brands_list_parallel()
