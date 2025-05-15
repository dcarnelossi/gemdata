import pandas as pd
from datetime import datetime, timedelta
from modules.dbpgconn import WriteJsonToPostgres
import logging
import time
import subprocess
import sys

from modules.api_conection import make_request_ga


# Função para instalar um pacote via pip
def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

# Instalar matplotlib se não estiver instalado
try:
    from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric,FilterExpression,Filter
 
except ImportError:
    print("google-analytics-data não está instalado. Instalando agora...")
    install("google-analytics-data")
    from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric,FilterExpression,Filter


# Instalar matplotlib se não estiver instalado
try:
    from google.api_core.exceptions import InvalidArgument
except ImportError:
    print("google-api-core não está instalado. Instalando agora...")
    install("google-api-core")
    from google.api_core.exceptions import InvalidArgument





# Variáveis globais
api_conection_info = None
data_conection_info = None
coorp_conection_info = None
client= None



# === CONFIG ===
# SERVICE_ACCOUNT_FILE = "C:/Python/Teste_api_shopify/proven-entropy-451418-b6-aadd279357bc.json"
property_id = "481670222"

# === MÉTRICAS DESEJADAS ===
metrics_list = ["sessions", "purchaseRevenue"]

dimensions_list = [
    "sessionSource", "sessionMedium", "sessionSourceMedium", "campaignName"
]


def get_account():
    try:
        return make_request_ga(api_conection_info)
    
    except Exception as e:
        logging.error(f"Failed to retrieve orders list pages: {e}")
        raise  # Rethrow the exception to signal the Airflow task failure



def run_report_ga(start_date, end_date):


    try:
        request = RunReportRequest(
            property=f"properties/{property_id}",
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimensions=[Dimension(name=d) for d in dimensions_list],
            metrics=[Metric(name=m) for m in metrics_list],
             dimension_filter=FilterExpression(
            filter=Filter(
                field_name="eventName",
                string_filter=Filter.StringFilter(value="purchase")
            )
        ),
            limit=100000
        )

        response = client.run_report(request)
        data = []

        for row in response.rows:
            row_data = {}
            for idx, dim in enumerate(dimensions_list):
                if dim == "date":
                    row_data["creationdate"] = row.dimension_values[idx].value
                else:
                    row_data[dim] = row.dimension_values[idx].value

            for idx, metric in enumerate(metrics_list):
                val = row.metric_values[idx].value
                row_data[metric] = float(val) if val else None

            data.append(row_data)

        return data

    except InvalidArgument as e:
        print(f"❌ Erro ao rodar o relatório: {e.message}")
        return []
    


def process_ga_batch(data, table, keytable):
    try:
        writer = WriteJsonToPostgres(
            data_conection_info,
            data,
            table,
            keytable
        )
        a=writer.upsert_data_batch(isdatainsercao=1)
        print(a)
       # logging.info(f"{len(order_list)} pedidos upsertados com sucesso.")
    except Exception as e:
        #logging.error(f"Erro ao upsertar lote de pedidos: {e}")
        raise

def  execute_process_ga (start_date, end_date):
    
    data_ga4 = run_report_ga(start_date,end_date)

    if data_ga4:  # só tenta processar se tiver dados
        # data_conection_info = "integrations-data-dev"  # ajuste se for produção
        # schema = "d171441b-f439-49ef-9be5-9eba36ab7d72"
        table = "ga_traffic_purchase"
        keytable = "creationdate, sessionsource, sessionmedium, sessionsourcemedium, campaignname"

        process_ga_batch(data_ga4, table, keytable)
        print("✅ Inserido com sucesso.")
        # (Opcional) salvar como CSV
        # import pandas as pd
        # df = pd.DataFrame(data_ga4)
        # df.to_csv("ga4_dados_ano_completo.csv", index=False)
      #  print("✅ CSV exportado com sucesso.")
    else:
        print("⚠️ Nenhum dado retornado para processar.")


def date_process(start_date, end_date, delta=None):
    try:
        start_time = time.time()
        if delta:
            end_date_d = datetime.now()
            end_date= end_date_d.strftime("%Y-%m-%d")
            start_date = (end_date_d - timedelta(days=delta)).strftime("%Y-%m-%d")
        
  
                # Se vierem como string, converter para datetime
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d")

        # Agora sim formatar
        start_date = start_date.strftime("%Y-%m-%d")
        end_date = end_date.strftime("%Y-%m-%d")

        execute_process_ga(start_date, end_date)
        logging.info("Processamento concluído com sucesso.")
    except Exception as e:
        logging.error(f"Erro durante a execução: {e}")
        raise
    finally:
        logging.info(f"Tempo total de execução: {time.time() - start_time:.2f} segundos.")    



def set_globals(api_info, data_conection, start_date,end_date):
    global api_conection_info
    api_conection_info = api_info

    global data_conection_info
    data_conection_info = data_conection

    global client
    
  
    try:
        client_account=get_account()
        
        client = client_account
        
        date_process(start_date,end_date)
    except Exception as e:
        raise e





