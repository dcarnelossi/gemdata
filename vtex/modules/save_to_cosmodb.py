
from vtex.modules.config import *

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