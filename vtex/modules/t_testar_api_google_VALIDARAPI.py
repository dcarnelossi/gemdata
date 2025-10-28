
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.oauth2 import service_account

# Caminho para o arquivo JSON da conta de serviço
SERVICE_ACCOUNT_FILE = "C:/Python/github-gemdataprod/gemdata/proven-entropy-451418-b6-aadd279357bc.json"

# ID da propriedade GA4 (substitua pelo seu ID real)
PROPERTY_ID = "481670222"

# Autenticação com a conta de serviço
try:
    credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
    client = BetaAnalyticsDataClient(credentials=credentials)
    print("✅ Conexão bem-sucedida! A API está ativa e autenticada.")
except Exception as e:
    print(f"❌ Erro na conexão com a API: {e}")
