from airflow.hooks.base import BaseHook
from azure.storage.blob import BlobServiceClient

class ExecuteBlob:
    def __init__(self):
        # Obter a conexão cadastrada no Airflow
        connection = BaseHook.get_connection('Blob_Store_Geral')

        # Obter a connection string da conexão cadastrada
        connection_string = connection.extra_dejson.get('connection_string')

        if not connection_string:
            raise ValueError("Connection string para o Azure Blob Storage não está configurada corretamente.")

        # Criar o BlobServiceClient usando a connection string obtida
        self.connblob = BlobServiceClient.from_connection_string(connection_string)

    def get_file(self, container_name, blob_name, download_path):
        """
        Função para baixar um arquivo do Azure Blob Storage
        """
        try:
            # Criar um cliente de blob
            blob_client = self.connblob.get_blob_client(container=container_name, blob=blob_name)
            
            # Baixar o arquivo
            with open(download_path, "wb") as download_file:
                download_file.write(blob_client.download_blob().readall())
            print(f"Arquivo {blob_name} baixado com sucesso para {download_path}")
        except Exception as e:
            print(f"Erro ao baixar o arquivo: {e}")
            


    def upload_file(self, container_name, blob_name, file_path):
        """
        Função para fazer upload de um arquivo para o Azure Blob Storage
        """
        try:
            # Criar um cliente de blob
            blob_client = self.connblob.get_blob_client(container=container_name, blob=blob_name)
            
            # Carregar o arquivo
            with open(file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)  # overwrite=True para substituir se já existir
            print(f"Arquivo {file_path} enviado com sucesso para o blob {blob_name} no container {container_name}")
        except Exception as e:
            print(f"Erro ao enviar o arquivo: {e}")

