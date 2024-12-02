import logging
from teste_dbpgconn import WriteJsonToPostgres
import pandas as pd 
from datetime import datetime

import sqlscripts as sq

import sqlscriptabglobal as sqlglobal

def criando_estrutura_shopify(schema):
    
    sql_script = sq.shopifysqlscripts(schema, "adminuserapppggemdataprod")
    
    WriteJsonToPostgres("integrations-data-prod", sql_script).execute_query()




def criando_tabelas_IA_shopify(schema):
    
    sql_script = sqlglobal.shopifysqlscriptscreatetabglobal(schema)
    
    WriteJsonToPostgres("integrations-data-prod", sql_script).execute_query()




def trunca_tabelas_IA (schema):

    
    query= f"""
                   truncate table "{schema}".orders_ia
                    
    """

    WriteJsonToPostgres("integrations-data-prod", query).execute_query()

    
    query2= f"""
                  truncate table "{schema}".orders_items_ia
                    
    """

    WriteJsonToPostgres("integrations-data-prod", query2).execute_query()



    query3= f"""
                   truncate table "{schema}".orders_ia_forecast
                    
    """

    WriteJsonToPostgres("integrations-data-prod", query3).execute_query()



def delete_tabelas_IA (schema,createdat):

    
    query= f"""
                    delete from "{schema}".orders_ia where creationdate >='{createdat}'
                    
    """

    WriteJsonToPostgres("integrations-data-prod", query).execute_query()

    
    query2= f"""
                  delete from "{schema}".orders_items_ia  where creationdate >='{createdat}'
                    
    """

    WriteJsonToPostgres("integrations-data-prod", query2).execute_query()




 
def process_order(nometabela,schema,order):
    try:    
        writer = WriteJsonToPostgres(
                    "integrations-data-prod", order, f""""{schema}".{nometabela}""", "orderid"
                )

        writer.insert_data()
        logging.info(f"Order {order['orderId']} upserted successfully.")
    except Exception as e:
        logging.error(f"Error inserting order {order['orderId']}: {e}")
        raise  # Ensure failure is propagated to Airflow


def importar_orderia_shopify(schema,schema_copy,data_create):
    dataini_total = datetime.now()    
    # Consulta para puxar os dados
    query_consulta = f""" 
    SELECT * FROM "{schema_copy}".orders_ia where creationdate>= '{data_create}'
    """
   # print(query_consulta)

    # Executa a consulta e obtém os resultados
    _, result = WriteJsonToPostgres("integrations-data-dev", query_consulta, "orders_ia").query()
    sales_df = pd.DataFrame(result)

    # Processar os dados em lotes
    batch_size = 500
    num_batches = len(sales_df) // batch_size + (1 if len(sales_df) % batch_size > 0 else 0)

    for batch_number in range(num_batches):
        # Define o índice do lote atual
        dataini = datetime.now()
        start_index = batch_number * batch_size
        end_index = start_index + batch_size
        batch_df = sales_df.iloc[start_index:end_index]  # Divide o DataFrame em um lote
        
        # Converte para lista de dicionários
        batch = batch_df.to_dict(orient='records')
        
        print(f"Processando lote {batch_number + 1}/{num_batches} com {len(batch)} registros.")
        
        # Insere os dados do lote
        WriteJsonToPostgres(
            "integrations-data-prod",
            batch,
            f""""{schema}".orders_ia""",
            "orderid"
        ).insert_data_batch(batch)
        
        datafim = datetime.now()
        print(dataini)
        print(datafim) 
        tempo_processamento= datafim - dataini
        print(f"Tempo de processamento Total: {tempo_processamento}")
     
    datafim_total = datetime.now() 
    tempo_processamento_total=datafim_total - dataini_total
    print(dataini_total)
    print(datafim_total) 
    print(f"Tempo de processamento Total: {tempo_processamento_total}")
 


def importar_orderitemia_shopify(schema,schema_copy,data_create):
    
      
    dataini_total = datetime.now()
    query_consulta =f""" 
    select *  from "{schema_copy}".orders_items_ia where creationdate>= '{data_create}'
                        """
        
    print(query_consulta)
    _, result = WriteJsonToPostgres("integrations-data-dev", query_consulta, "orders_items_ia").query()
    
    sales_df = pd.DataFrame(result)

    # Processar os dados em lotes
    batch_size = 500
    num_batches = len(sales_df) // batch_size + (1 if len(sales_df) % batch_size > 0 else 0)

    for batch_number in range(num_batches):
        dataini = datetime.now()
        # Define o índice do lote atual
        start_index = batch_number * batch_size
        end_index = start_index + batch_size
        batch_df = sales_df.iloc[start_index:end_index]  # Divide o DataFrame em um lote
        
        # Converte para lista de dicionários
        batch = batch_df.to_dict(orient='records')
        
        print(f"Processando lote {batch_number + 1}/{num_batches} com {len(batch)} registros.")
        
        # Insere os dados do lote
        WriteJsonToPostgres(
            "integrations-data-prod",
            batch,
            f""""{schema}".orders_items_ia""",
            "orderid"
        ).insert_data_batch(batch)
        datafim = datetime.now()
        print(dataini)
        print(datafim) 
        tempo_processamento= datafim - dataini
        print(f"Tempo de processamento Total: {tempo_processamento}")
     
    datafim_total = datetime.now() 
    tempo_processamento_total=datafim_total - dataini_total
    print(dataini_total)
    print(datafim_total) 
    print(f"Tempo de processamento Total: {tempo_processamento_total}")
 



def importar_forecastia_shopify(schema,schema_copy):
    
   
    query_consulta =f""" 
    select *  from "{schema_copy}".orders_ia_forecast
                        """
        
    print(query_consulta)
    _, result = WriteJsonToPostgres("integrations-data-dev", query_consulta, "orders_ia_forecast").query()
    

    #_, result = WriteJsonToPostgres("integrations-data-dev", query, "orders_ia").query()
    sales_df =  pd.DataFrame(result)
   
    
    
    bath=sales_df.to_dict(orient='records')

    #print(bath)
     
    WriteJsonToPostgres("integrations-data-prod", bath,f""""{schema}".orders_ia_forecast""","orders_ia_forecast").insert_data_batch(bath)
   

schema = "e7217b31-b471-4d59-957b-fb06b1e9f8fd"

schema_copy = "07e14abb-bf37-4022-b159-03d2a757b40b"

start_date= '2024-10-20'


#criando_estrutura_shopify(schema)

#criando_tabelas_IA_shopify(schema)

#trunca_tabelas_IA(schema)

#delete_tabelas_IA(schema,start_date)

importar_orderia_shopify(schema,schema_copy,start_date)

importar_orderitemia_shopify(schema,schema_copy,start_date)

# importar_forecastia_shopify(schema,schema_copy)

