
import os
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
import json


# def save_json_to_blob_storage(file_dir,file_name,string_to_save):
#     try:
#         # Connect to Azure Blob Storage service
#         blob_connection = BlobServiceClient(
#             account_url=f"https://{account_name}.blob.core.windows.net",
#             credential=account_key
#         )

#         # Get a reference to the container
#         container_client = blob_connection.get_container_client(container=container_name)

#         # Convert the string to a JSON object
#         json_object = json.loads(string_to_save)

#         # Convert the JSON object to a formatted JSON string
#         formatted_json_string = json.dumps(json_object, indent=2)

#         # Create or replace the blob in the container with the formatted JSON string
#         blob_client = container_client.get_blob_client(blob=f"{file_dir}/{file_name}.json")
#         blob_client.upload_blob(formatted_json_string, overwrite=True)

#         print(f'The file {file_name} was successfully saved to Blob Storage.')

#     except ResourceNotFoundError:
#         print(f'The container {container_name} was not found.')
#     except ResourceExistsError:
#         print(f'The blob {file_name} already exists in the container {container_name}. Use another file name.')
#     except Exception as e:
#         print(f'save_json_to_blob_storage - An unexpected error occurred: {e}')



class execute_blob():

    def __init__(self):
        load_dotenv()
        self.blob = os.getenv(f"BLOB_DATA")
        self.connblob =  BlobServiceClient.from_connection_string(self.blob)

    def insert_file(self,container,filename,file):
        try:
            blob_client =  self.connblob.get_blob_client(container=container, blob=filename)
            with open(file, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
            return True
        except Exception:
            return False
    
    def get_file(self,container,filename,file):
        try:
            blob_client = self.connblob.get_blob_client(container=container, blob=filename)
            with open(file, "wb") as download_file:
                download_file.write(blob_client.download_blob().readall())
        except Exception as e:
            return e
        

    
