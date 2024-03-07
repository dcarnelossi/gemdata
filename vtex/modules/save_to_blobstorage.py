
from vtex.modules.config import *

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
