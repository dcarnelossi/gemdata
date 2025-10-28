from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Metric, Dimension
from google.oauth2 import service_account

# Caminho para o arquivo de credenciais JSON
SERVICE_ACCOUNT_FILE = "C:/Python/github-gemdataprod/gemdata/proven-entropy-451418-b6-aadd279357bc.json"

# ID da propriedade GA4 (substitua pelo seu ID)
PROPERTY_ID = "481670222"

# Autenticação com a conta de serviço
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)

# Criando cliente
client = BetaAnalyticsDataClient(credentials=credentials)

# Criando a solicitação (com métricas válidas)
request = RunReportRequest(
    property=f"properties/{PROPERTY_ID}",
    date_ranges=[DateRange(start_date="2024-01-01", end_date="today")],
    metrics=[
        Metric(name="sessions"),  # Sessões
        Metric(name="activeUsers"),  # Usuários ativos
        Metric(name="addToCarts")  # Carrinhos adicionados (sugestão do erro)
    ],
    dimensions=[Dimension(name="date")],
)

# Executando a solicitação
response = client.run_report(request)

# Exibir os resultados
print("Resultados do Google Analytics:")
for row in response.rows:
    print(f"Data: {row.dimension_values[0].value} - Sessões: {row.metric_values[0].value} - Usuários Ativos: {row.metric_values[1].value} - AddToCarts: {row.metric_values[2].value}")
