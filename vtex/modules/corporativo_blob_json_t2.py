import json
import os
import psycopg2
from psycopg2 import sql



from datetime import datetime

import psycopg2.extensions


from azure.storage.blob import BlobServiceClient, BlobClient
from azure.storage.fileshare import  ShareFileClient
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError


#pip install azure-storage-blob
from vtex.modules.blobconn import *

blob_conection_info=None




db_params = {
    'host': '20.98.214.213',
    'database': 'db-vetex-dev-00',
    'user': 'adminuserpggemdatadev',
    'password': 'qgfC64psgk7CCveWRFHAPCQR0F3DPzdxIUW7uD2HaHxuG0MoemnvpHYlCeM5',
    'port': '5432',
}




# Função para criar uma conexão com o PostgreSQL
def criar_conexao():
    try:
        # Conecta ao banco de dados
        conexao = psycopg2.connect(**db_params)
        print("Conexão bem-sucedida!")
        return conexao
    except Exception as e:
        print(f"Erro ao conectar ao PostgreSQL: {e}")
        return None
    
#######################################################
#aqui faz a blob



# Exemplo de execução de uma consulta para json 
def executar_query_json(consulta,args=(), one= False):
    try:
        # Cria um cursor para executar a consulta
        #exemplo isso aqui criar conexao é DEF
        cur = criar_conexao().cursor()
        # Executa a consulta
        cur.execute(consulta, args)
        r = [dict((cur.description[i][0], value) \
               for i, value in enumerate(row)) for row in cur.fetchall()]
        cur.connection.close()
        return (r[0] if r else None) if one else r
            
    except Exception as e:
        print(f"Erro ao executar a consulta: {e}")

    finally:
        # Fecha o cursor e a conexão
        cur.close()
        cur.connection.close()
        print("Conexão fechada.")



#criar_query_json_arquivo()

#teste_caminho= diretorio_json('teste1','a.json')
#print(teste_caminho)
#my_query = executar_consulta("select cast(round((cast(oi.price as numeric)/100),	2 ) as text) as price from mahogany.orders_items oi limit 1 ")



#print (my_query)

#with open(teste_caminho, 'w') as file:
#    json.dump(my_query, file)




#file_dir,file_name,string_to_save
def save_json_to_blob_storage(file_name,consulta):
    try:
        # Connect to Azure Blob Storage service
        blob_connection = BlobServiceClient(
            account_url=f"https://{'servicejson'}.blob.core.windows.net",
            credential="ngj+AnjQWnNY5E/VqyaNUYyC+pRLkrnuIpKeauwnY7hPi4TQC+g8x319/cy0Y2Aq8WFrPvKsSUoL+AStUmxhHA=="
        )

        #def create_blob_container(self, blob_service_client: BlobServiceClient, container_name):
        try:
            container_client = blob_connection.create_container(name=coorp_id_create_structure )
        except ResourceExistsError:
            container_client = blob_connection.get_container_client(container=coorp_id_create_structure)

        
        # Create or replace the blob in the container with the formatted JSON string
        blob_client = container_client.get_blob_client(file_name)
       
        query_json=executar_query_json(consulta)

        #esse metodo é mais lento, mas no blob só consigo fazer esse. 
        resuljson = json.dumps(query_json)
       
        blob_client.upload_blob(resuljson, blob_type="BlockBlob", overwrite=True)        

       # print(f'The file {file_name} was successfully saved to Blob Storage.')

    except ResourceNotFoundError:
        print(f'The container {container_name} was not found.')
    except ResourceExistsError:
        print(f'The blob {file_name} already exists in the container {container_name}. Use another file name.')
    except Exception as e:
        print(f'save_json_to_blob_storage - An unexpected error occurred: {e}')









#=======================================================
#só mudar a query que ele vai gerar uma pasta no teu desktop chama XXX e o arquivo vai estar la dentro 
#fiz isso mais para voce gerar por enquanto os exemplo que precisa.. se precisa de ajudar com query me avisa 
#estou fazendo a parte de storage azure agora, ai depois entro nas view que vão gerar esse json .. futuramente vai 
#ser o seguinte : select * from schema.vw_json_nomegrafic ou ou nuemro do grafico.
#criar_query_json_arquivo (nome do arquivo, e o select )

query_json=executar_query_json(""" select distinct 
cast( to_char(o.invoiceddate,'YYYYMMDD') as bigint) as datainvoicenum,
cast( to_char(o.invoiceddate,'DD/MM/YYYY') as text) as datainvoicebrasil
from orders o 
where statusdescription = 'Faturado'

""")

        #esse metodo é mais lento, mas no blob só consigo fazer esse. 
resuljson = json.dumps(query_json)


ActionBlob.blobupload(blob_conection_info, "teste","testedata.json",resuljson)






