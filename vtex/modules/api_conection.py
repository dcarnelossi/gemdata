import http.client
import logging

import requests
import base64

import subprocess
import sys
# Função para instalar um pacote via pip
def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])


# Instalar matplotlib se não estiver instalado
try:
    from google.oauth2 import service_account
except ImportError:
    print("google-auth não está instalado. Instalando agora...")
    install("google-auth")
    from google.oauth2 import service_account


# Instalar matplotlib se não estiver instalado
try:
    from google.analytics.data_v1beta import BetaAnalyticsDataClient
except ImportError:
    print("google-analytics-data não está instalado. Instalando agora...")
    install("google-analytics-data")
    from google.analytics.data_v1beta import BetaAnalyticsDataClient



def vtex_test_conection(domain, method, path, params=None, headers=None):
    try:
        conn = http.client.HTTPSConnection(domain)

        conn.request("GET", "api/catalog_system/pub/category/tree/1", headers=headers)

        res = conn.getresponse()
        data = res.read()

        if res.status == 200:
            print(data)
            return data
        else:
            print(f"Error: {res.status} - {res.reason} - {res}")
            return None

    except http.client.HTTPException as http_error:
        print(f"vtex_test_conection - HTTPException: {http_error}")
        return None
    except Exception as e:
        print(f"vtex_test_conection - Error: {e}")
        return None
    finally:
        conn.close()


session = requests.Session()


def make_request(domain, method, path, params=None, headers=None,json=None):
    try:
        
        
        response = session.request(
            method, f"https://{domain}/{path}", params=params, headers=headers,json=json
        )
        # print(response)
        # print (response.json())
        return response.json() if response.status_code == 200 else None
    except requests.RequestException as e:
        logging.error(f"Request failed: {e}")
        raise


def make_request_ga(account_file):
    try:
        
        credentials = service_account.Credentials.from_service_account_info(account_file)
        client = BetaAnalyticsDataClient(credentials=credentials)

        return client 
    except requests.RequestException as e:
        logging.error(f"Request failed: {e}")
        raise


def make_request_token_moovin(apikey,apisecret,accountclientid):
    try:

        # Monta o header Authorization Basic
        user_pass = f"{apikey}:{apisecret}"
        basic_auth = base64.b64encode(user_pass.encode()).decode()

        headers = {
            '1eg-Account-Id': accountclientid,
            '1eg-User-RoleType': 'MANAGER',
            '1eg-App-Auth': 'true',
            'Authorization': f'Basic {basic_auth}',
            'Accept': 'application/json'
        }

        response = requests.post("https://api.moovin.store/iam-manager/authentication", headers=headers)
        response.raise_for_status()

        token_info = response.json()
        print(token_info)
        # Verifique o campo correto onde vem o token na resposta (ex: 'access_token', 'token', etc)
        access_token = token_info['token']  
        return access_token

    except requests.RequestException as e:
        logging.error(f"Token request failed: {e}")
        raise



def make_request_api_moovin(access_token,url_api,limit):
    try:

        headers = {
                    'X-Authorization': f'Bearer {access_token}',
                    'Accept': 'application/json'
                }

        response = requests.get(url_api, headers=headers)
       

        return response.json() if response.status_code == 200 else None


    except requests.RequestException as e:
        logging.error(f"Token request failed: {e}")
        raise





def make_request_token_nuvem(domain, path,headers,page):
    try:
        
        response = requests.get(
            f"{domain}/{path}",
            headers=headers,
            params={'page': page, 'per_page':50}
        )
        
        
        return response.json() if response.status_code == 200 else None

    except requests.RequestException as e:
        logging.error(f"Request failed: {e}")
        raise

# if __name__ == "__main__":
#     vtex_test_conection(1)
