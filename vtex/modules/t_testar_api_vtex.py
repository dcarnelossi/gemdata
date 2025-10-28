import requests

# Defina as credenciais e o nome da conta
app_key = 'vtexappkey-tfbmsm-SUCPVP'
app_token = 'LRIXMTIPGGZBTYKVQGKARMELAZZEEXFTFHGLJCSVQLJTODYKEYUNSQIVGRUMBTTEMNXGGFSZCBUDTJTKZTYXRCUIMGEFZPSDPWNBLXOGKUKSZVZSWCVSYCYWCVERCXUR'
account_name = 'tfbmsm'
environment = 'vtexcommercestable'   # ou 'vtexcommercebeta', conforme o ambiente

# URL base da API de Orders
url_base = f'https://{account_name}.{environment}.com.br/api/oms/pvt/orders'


data_ini = '2024-07-21T02:00:00.000000Z'

data_fim = '2024-07-22T01:59:59.999000Z'


# {'per_page': 100, 'f_creationDate': 'creationDate:[2024-09-19T02:00:00.000000Z TO 2024-09-20T01:59:59.999000Z]'}
# Parâmetros para filtrar os pedidos
params = {
     
    'per_page': 100, 'f_creationDate': 'creationDate:[2024-10-09T02:00:00.000000Z TO 2024-10-10T01:59:59.999000Z]'
 
      # Número de pedidos por página
}

# Cabeçalhos com as credenciais de autenticação
headers = {
    'X-VTEX-API-AppKey': app_key,
    'X-VTEX-API-AppToken': app_token,
    'Accept': 'application/json'
}

# Realiza a requisição GET
response = requests.get(url_base, headers=headers, params=params)

# Verifica se a requisição foi bem-sucedida
if response.status_code == 200:
    pedidos = response.json()
    for pedido in pedidos['list']:
        print(f"ID do Pedido: {pedido['orderId']}, Data: {pedido['creationDate']}")
else:
    print(f"Erro: {response.status_code} - {response.text}")
