
# pip install --upgrade google-auth-oauthlib google-analytics-data pandas
from google.analytics.data import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from google.oauth2 import service_account
import pandas as pd

# Carregar as credenciais da conta de serviço
credentials = service_account.Credentials.from_service_account_file('C:/Python/github-gemdataprod/gemdata/proven-entropy-451418-b6-aadd279357bc.json')

# Inicializar o cliente da API
client = BetaAnalyticsDataClient(credentials=credentials)

# Definir o ID da propriedade do GA4cl
property_id = '481670222'

# Configurar a solicitação do relatório
request = RunReportRequest(
    property=f"properties/{property_id}",
    dimensions=[
        Dimension(name="campaignName"),  # Nome da campanha
        Dimension(name="date"),          # Data
    ],
    metrics=[
        Metric(name="adCost"),           # Custo do anúncio
       # Metric(name="sessions"),
        Metric(name="conversions"),      # Conversões
        Metric(name="screenPageViews"),  # Visualizações de página
        Metric(name="clickThroughRate"), # Taxa de cliques
    ],
    date_ranges=[DateRange(start_date="2025-03-10", end_date="2025-03-12")],  # Intervalo de datas
)

# Executar a solicitação
response = client.run_report(request)

# Processar a resposta e converter em DataFrame
def response_to_dataframe(response):
    rows = []
    for row in response.rows:
        rows.append({
            "campaignName": row.dimension_values[0].value,
            "date": row.dimension_values[1].value,
            "adCost": float(row.metric_values[0].value),
            "conversions": int(row.metric_values[1].value),
            "screenPageViews": int(row.metric_values[2].value),
            "clickThroughRate": float(row.metric_values[3].value),
        })
    return pd.DataFrame(rows)

# Obter os dados em um DataFrame
df = response_to_dataframe(response)

# Exibir o DataFrame
print(df)
