import logging
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.models.param import Param

# Lista de requisitos
requirements = [
    "openai==1.6.0",
    "azure-core==1.29.6",
    "azure-cosmos==4.5.1",
    "azure-storage-blob==12.19.0",
]

# Configuração padrão do DAG
default_args = {
    "owner": "Daniel",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
}


with DAG(
    "ImportVtex-Orders-List",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["vtex", "orders-list", "IMPORT"],
    params={
        "PGSCHEMA": Param(
            type="string",
            title="PGSCHEMA:",
            description="Enter the integration PGSCHEMA.",
            section="Important params",
            min_length=1,
            max_length=200,
        )
    },
) as dag:
    from modules.dbpgconn import WriteJsonToPostgres

    def get_coorp_conection_info():
        coorp_conection_info = {
            "host": Variable.get("COORP_PGHOST"),
            "user": Variable.get("COORP_PGUSER"),
            "port": 5432,
            "database": Variable.get("COORP_PGDATABASE"),
            "password": Variable.get("COORP_PGPASSWORD"),
            "schema": "public",
        }

        return coorp_conection_info

    def get_data_conection_info(team_id):
        data_conection_info = {
            "host": Variable.get("DATA_PGHOST"),
            "user": Variable.get("DATA_PGUSER"),
            "port": 5432,
            "database": Variable.get("DATA_PGDATABASE"),
            "password": Variable.get("DATA_PGPASSWORD"),
            "schema": team_id,
        }

        return data_conection_info

    def integrationInfo(connection_info, team_id):
        try:
            print("integrationInfo")

            start_time = time.time()

            # postgres_conn = dbpgconn.PostgresConnection(connection_info)

            query = f"""SELECT
                            integration.*
                        FROM
                            public.integrations_integration AS integration
                        JOIN
                            public.teams_team AS team
                        ON integration.team_id = team.id
                        WHERE
                            team.slug = '{team_id}'
                        AND
                            integration.is_active = TRUE;
                        """

            select = WriteJsonToPostgres(connection_info, query)
            result = select.query()

            if result:
                return result[1]
            else:
                logging.error(
                    f"Importação das BRANDS deu pau. Tempo de execução: \
                    {time.time() - start_time:.2f} segundos"
                )
                return False
        except Exception as e:
            logging.exception("An unexpected error occurred during BRANDS import" - e)
            raise

    def get_api_conection_info(team_id):
        try:
            print(team_id)

            connection_info = get_coorp_conection_info()

            data = integrationInfo(connection_info, team_id)

            print(data)

            api_conection_info = data[0]

            print(api_conection_info)
            # VTEX_API_AppKey = api_conection_info['vtex_api_appkey']

            headers = {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "X-VTEX-API-AppKey": api_conection_info["vtex_api_appkey"],
                "X-VTEX-API-AppToken": api_conection_info["vtex_api_apptoken"],
            }

            vtexapi = {
                "VTEX_Domain": f"{api_conection_info['vtex_api_accountname']}.{api_conection_info['vtex_api_environment']}.com.br",
                "headers": headers,
            }

            return vtexapi

        except Exception as e:
            logging.exception(f"An unexpected error occurred during DAG - {e}")
            raise

    def get_import_last_rum_date(connection_info, team_id):
        try:
            print("get_import_last_rum_date")

            # postgres_conn = dbpgconn.PostgresConnection(connection_info)

            query = f"""SELECT
                            integration.import_last_run_date
                        FROM
                            public.integrations_integration AS integration
                        JOIN
                            public.teams_team AS team
                        ON integration.team_id = team.id
                        WHERE
                            team.slug = '{team_id}'
                        AND
                            integration.is_active = TRUE;"""

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


    @task(provide_context=True)
    def orders_list(**kwargs):
        team_id = kwargs["params"]["PGSCHEMA"]

        coorp_conection_info = get_coorp_conection_info()
        data_conection_info = get_data_conection_info(team_id)
        api_conection_info = get_api_conection_info(team_id)
        last_rum_date = get_import_last_rum_date(coorp_conection_info, team_id)

        from modules import orders_list

        try:
            end_date = datetime.now()

            if last_rum_date[0]["import_last_rum_date"] is None:
                start_date = end_date - timedelta(days=730)
            else:
                start_date = last_rum_date["import_last_rum_date"] - timedelta(days=1)

            orders_list.set_globals(
                api_conection_info,
                data_conection_info,
                coorp_conection_info,
                start_date=start_date,
                end_date=end_date,
            )

            # Pushing data to XCom
            kwargs["ti"].xcom_push(key="team_id", value=team_id)
            kwargs["ti"].xcom_push(
                key="coorp_conection_info", value=coorp_conection_info
            )
            kwargs["ti"].xcom_push(key="data_conection_info", value=data_conection_info)
            kwargs["ti"].xcom_push(key="api_conection_info", value=api_conection_info)

            return True
        except Exception as e:
            logging.exception(f"An unexpected error occurred during DAG - {e}")
            raise e
        finally:
            pass

    # Configurando a dependência entre as tasks

    orders_list_task = orders_list()
    orders_list_task
