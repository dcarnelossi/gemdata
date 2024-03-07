from vtex.modules.config import *

def get_categories_tree(category_levels):
    try:
        conn = http.client.HTTPSConnection(VTEX_Domain)

        conn.request("GET", f"/api/catalog_system/pub/category/tree/{category_levels}", headers=headers)

        res = conn.getresponse()
        data = res.read()

        if res.status == 200:
            print(data.decode("utf-8"))
            #save_json_to_blob_storage("categories",data.decode("utf-8"))
            for id in extract_category_ids(data.decode("utf-8")):
                get_products_by_category(id)
            return data.decode("utf-8")
        else:
            print(f"Error: {res.status} - {res.reason}")
            return None

    except http.client.HTTPException as http_error:
        print(f"HTTPException: {http_error}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None
    finally:
        conn.close()


def get_products_by_category(category_id):
    try:
        conn = http.client.HTTPSConnection(VTEX_Domain)

        # Construa a string de consulta usando urlencode
        query_params = urlencode({
            "categoryId": category_id
            })
        
        conn = http.client.HTTPSConnection(VTEX_Domain)

        conn.request("GET", f"/api/catalog_system/pvt/products/GetProductAndSkuIds?{query_params}", headers=headers)

        res = conn.getresponse()
        data = res.read()
        
    
        if res.status == 200:
            print(data.decode("utf-8"))
            save_json_to_blob_storage("products", "products", data.decode("utf-8"))
            for id in extract_product_ids(data.decode("utf-8")):
                get_product_id(id)
            return data.decode("utf-8")
        else:
            print(f"Error: {res.status} - {res.reason}")
            return None

    except http.client.HTTPException as http_error:
        print(f"HTTPException: {http_error}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None
    finally:
        conn.close()
        

def get_product_id(product_id):
    
    try:
        conn = http.client.HTTPSConnection(VTEX_Domain)

        conn.request("GET", f"/api/catalog/pvt/product/{product_id}", headers=headers)

        res = conn.getresponse()
        data = res.read()

        if res.status == 200:
            print(data.decode("utf-8"))
            #save_json_to_blob_storage("products", f"product_{product_id}", data.decode("utf-8"))
            #save_json_to_cosmosdb("products", data.decode("utf-8"))
            writer = insdata.WriteJsonToPostgres(json.loads(data.decode("utf-8")), 'products')

            if not writer.table_exists():
                try:
                    writer.create_table()
                    print("criou tabela -----------------------------------------")
                except Exception as e:
                    print(f"Erro desconhecido - {e}")


            try:
                writer.insert_data()
                print("criou registro -----------------------------------------")
            except Exception as e:
                print(f"Erro desconhecido - {e}")     
            return data.decode("utf-8")
        else:
            print(f"Error: {res.status} - {res.reason}")
            return None
    except http.client.HTTPException as http_error:
        print(f"HTTPException: {http_error}")
        return None
    except Exception as e:
        print(f"get_product_id - {product_id} - An unexpected error occurred: {e}")
        return None
    finally:
        conn.close()
        
def extract_category_ids(category_list):
    # Carregar a string JSON
    dados_json = json.loads(category_list)

    # Função para extrair recursivamente os IDs
    def extrair_ids(objeto):
        ids = [objeto['id']]
        if 'children' in objeto:
            for child in objeto['children']:
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
    print("---------------")
    # Extrair os primeiros objetos do campo "data" para uma lista
    data_list = [item for sublist in dados_json["data"].values() for item in sublist]
    print("---------------")
    print(data_list)
    return data_list

# Chamada da função para preencher a lista
#extract_product_ids(json_data)




def save_json_to_cosmosdb(container_id, json_data):
    try:
        # Configurar suas credenciais e informações do Cosmos DB

        # Criar um cliente do Cosmos DB
        client = CosmosClient(endpoint, key)

        # Obter ou criar um banco de dados
        database = client.create_database_if_not_exists(id=database_id)

        # Definir o throughput desejado em RU/s (100 neste caso)
        throughput = 400

        # Obter ou criar um contêiner no banco de dados
        container = database.create_container_if_not_exists(
            id=container_id,
            partition_key=PartitionKey(path=f"/{database_id}"),
            offer_throughput=throughput
        )

        # Converter a string JSON para um dicionário
        json_data_dict = json.loads(json_data)

        # Corrigir a propriedade "Id" para "id" (minúsculo)
        if "Id" in json_data_dict:
            json_data_dict["id"] = str(json_data_dict.pop("Id"))
            

        # Adicionar um item ao contêiner
        container.create_item(body=json_data_dict)

        print("Item adicionado com sucesso ao contêiner.")

    except exceptions.CosmosResourceExistsError as e:
        print(f"Erro: O recurso já existe - {e}")

    except exceptions.CosmosHttpResponseError as e:
        print(f"Erro de resposta HTTP do Cosmos DB - {e}")

    except Exception as e:
        print(f"Erro desconhecido - {e}")

# Exemplo de uso da função
# database_id = "YOUR_DATABASE_ID"
# json_data = {"id": "2", "name": "Jane Doe", "age": 25}






from azure.storage.blob import BlobServiceClient
import json
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError

from azure.storage.blob import BlobServiceClient
import json
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError

def save_json_to_blob_storage(file_dir,file_name,string_to_save):
    try:
        # Connect to Azure Blob Storage service
        blob_connection = BlobServiceClient(
            account_url=f"https://{account_name}.blob.core.windows.net",
            credential=account_key
        )

        # Get a reference to the container
        container_client = blob_connection.get_container_client(container=container_name)

        # Convert the string to a JSON object
        json_object = json.loads(string_to_save)

        # Convert the JSON object to a formatted JSON string
        formatted_json_string = json.dumps(json_object, indent=2)

        # Create or replace the blob in the container with the formatted JSON string
        blob_client = container_client.get_blob_client(blob=f"{file_dir}/{file_name}.json")
        blob_client.upload_blob(formatted_json_string, overwrite=True)

        print(f'The file {file_name} was successfully saved to Blob Storage.')

    except ResourceNotFoundError:
        print(f'The container {container_name} was not found.')
    except ResourceExistsError:
        print(f'The blob {file_name} already exists in the container {container_name}. Use another file name.')
    except Exception as e:
        print(f'save_json_to_blob_storage - An unexpected error occurred: {e}')



get_products_by_category(4)