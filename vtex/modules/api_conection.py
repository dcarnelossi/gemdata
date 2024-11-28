import http.client
import logging

import requests


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


if __name__ == "__main__":
    vtex_test_conection(1)
