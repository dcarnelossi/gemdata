from vtex.modules.config import *



class BlobAzureConnection:
    def __init__(self, connection_info):
        load_dotenv()
        
        self.blobaccounturl = connection_info['blobaccounturl']
        self.blobcredential = connection_info['blobcredential']
        self.blobcontainer = connection_info['blobcontainer']
        self.blobclient = connection_info['blobclient']
        self.blob_connection = None
        self.blob_connection_container = None
        
    def connectblob(self):
        try:
            if self.blob_connection is None:
                self.blob_connection = BlobServiceClient(
                    account_url=self.blobaccounturl,
                    credential=self.blobcredential

                )
                # Added to set the connection to use transactions
            return self.blob_connection
        except Exception as e:
            print(f"Erro ao conectar no blob {self.blobaccounturl} : {e}")
            return False
    def connectcontainer(self):
        try:
            blob_connection = self.connectblob()
            print(blob_connection)
          #  print()
            self.blob_connection_container= blob_connection.get_container_client(container= self.blobcontainer)
            return self.blob_connection_container
        except Exception as e:
            print(f"Erro ao conectar no container {self.blobcontainer} : {e}")
            return False

class ActionBlob():

    def __init__(self, connection_info, namefile,file):
        self.connection = BlobAzureConnection(connection_info)
        self.namefile = namefile 
        self.file = file
        self.blobclient = connection_info['blobclient']


#    def bloblist(self)
#        blob_list = container.list_blobs()
#        for blob in blob_list:
#            lista=print(blob.name + '\n')
#        return lista

    def blobupload(self):
        try:
            with self.connection.connectcontainer() as connblob:
                blobfile = connblob.get_blob_client(f"{self.blobclient}/{self.namefile}")
                blobfile.upload_blob(self.file, blob_type="BlockBlob", overwrite=True)   
                return True
        except Exception as e:
            print(f"Erro ao salvar no blob {self.blobclient}/{self.namefile}: {e}")
            return False
      

