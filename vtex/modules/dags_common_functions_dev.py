import logging
import time

from airflow.models import Variable

from modules.dbpgconn import WriteJsonToPostgres


def get_coorp_conection_info():
    coorp_conection_info = {
        "host": Variable.get("DEV_COORP_PGHOST"),
        "user": Variable.get("DEV_COORP_PGUSER"),
        "port": 5432,
        "database": Variable.get("DEV_COORP_PGDATABASE"),
        "password": Variable.get("DEV_COORP_PGPASSWORD"),
        "schema": "public",
    }

    return coorp_conection_info


def get_data_conection_info(integration_id):
    data_conection_info = {
        "host": Variable.get("DEV_DATA_PGHOST"),
        "user": Variable.get("DEV_DATA_PGUSER"),
        "port": 5432,
        "database": Variable.get("DEV_DATA_PGDATABASE"),
        "password": Variable.get("DEV_DATA_PGPASSWORD"),
        "schema": integration_id,
    }

    return data_conection_info


def integrationInfo(connection_info, integration_id):
    try:
        print("integrationInfo")

        start_time = time.time()

        # postgres_conn = dbpgconn.PostgresConnection(connection_info)

        query = f"""SELECT
                        *
                    FROM
                        public.integrations_integration 
                    WHERE
                        id = '{integration_id}'
                    AND
                        is_active = TRUE;
                    """

        select = WriteJsonToPostgres(connection_info, query)
        result = select.query()

        if result:
            return result[1]
        else:
            logging.error(
                f"Erro na def integrationinfo da dags_common_functions. Tempo de execução: \
                {time.time() - start_time:.2f} segundos"
            )
            return False
    except Exception as e:
        logging.exception("Erro na def integrationinfo da dags_common_functions" - e)
        raise


def get_api_conection_info(integration_id):
    try:
        print(integration_id)

        connection_info = get_coorp_conection_info()

        data = integrationInfo(connection_info, integration_id)

        print(data)

        api_conection_info = data[0]

        print(api_conection_info)
        # VTEX_API_AppKey = api_conection_info['vtex_api_appkey']
        if(api_conection_info["hosting"]=='vtex'):
            headers = {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "X-VTEX-API-AppKey": api_conection_info["api_appkey"],
                "X-VTEX-API-AppToken": api_conection_info["api_apptoken"],
            }

            apicliente = {
                "Domain": f"{api_conection_info['api_accountname']}.{api_conection_info['api_environment']}.com.br",
                "headers": headers,
            }
        elif (api_conection_info["hosting"]=='shopify'):
            #shopify    
            headers = {
                "Content-Type": "application/json",
                'X-Shopify-Access-Token': api_conection_info["api_apptoken"],
            }

            apicliente = {
                "Domain": f"{api_conection_info['api_accountname']}.{api_conection_info['api_environment']}.com",
                "headers": headers,
            }
        else:
            apicliente = {
                "apptoken": 'b8f67d126c58b69b8f66',
                "appapplication": 'a3ed7334-73c0-44c4-9461-bdfa37b1403c',
            }

    #   "apptoken": api_conection_info["api_apptoken"],
          
        return apicliente


    except Exception as e:
        logging.exception(f"An unexpected error occurred during DAG - {e}")
        raise


def get_import_last_rum_date(connection_info, integration_id):
    try:
        print("get_import_last_rum_date")

        # postgres_conn = dbpgconn.PostgresConnection(connection_info)

        query = f"""SELECT
                        import_last_run_date
                    FROM
                        public.integrations_integration
                    WHERE
                        id = '{integration_id}'
                    AND
                        is_active = TRUE;"""

        result = WriteJsonToPostgres(connection_info, query).query()

        if result:
            print(result[1])
            return result[1]
        else:
            logging.error("Importação das get_import_last_rum_date deu pau")
            return False
    except Exception as e:
        logging.exception("An unexpected error occurred during BRANDS import" - e)
        raise


def update_import_last_rum_date(connection_info, integration_id):
    try:
        print("update_import_last_rum_date")

        # postgres_conn = dbpgconn.PostgresConnection(connection_info)

        query = f"""SELECT
                        import_last_run_date
                    FROM
                        public.integrations_integration
                    WHERE
                        id = '{integration_id}'
                    AND
                        is_active = TRUE;"""

        result = WriteJsonToPostgres(connection_info, query).query()

        if result:
            print(result[1])
            return result[1]
        else:
            logging.error("Importação das get_import_last_rum_date deu pau")
            return False
    except Exception as e:
        logging.exception("An unexpected error occurred during BRANDS import" - e)
        raise