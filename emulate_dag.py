import logging
import os
import time
from datetime import datetime, timedelta

from dotenv import load_dotenv

from vtex.modules import dbpgconn

load_dotenv()


def integrationInfo(connection_info, integration_id):
    try:
        print("integrationInfo")

        start_time = time.time()

        # postgres_conn = dbpgconn.PostgresConnection(connection_info)

        query = f"""SELECT *
                    FROM public.integrations_integration
                    WHERE id = '{integration_id}';"""

        select = dbpgconn.WriteJsonToPostgres(connection_info, query)
        result = select.query()

        if result:
            logging.info(
                f"Importação das BRANDS Concluída com sucesso. \
                    Tempo de execução: {time.time() - start_time:.2f} segundos"
            )
            logging.info(f"Tempo de execução: {time.time() - start_time:.2f}")
            print(result)
            return (result,)
        else:
            logging.error(
                f"Importação das BRANDS deu pau. \
                    Tempo de execução: {time.time() - start_time:.2f} segundos"
            )
            return False
    except Exception as e:
        logging.exception("An unexpected error occurred during BRANDS import" - e)
        return False


def get_import_last_rum_date(connection_info, integration_id):
    try:
        print("get_import_last_rum_date")

        # postgres_conn = dbpgconn.PostgresConnection(connection_info)

        query = f"""SELECT import_last_rum_date
                    FROM public.integrations_integration
                    WHERE id = '{integration_id}';"""

        select = dbpgconn.WriteJsonToPostgres(connection_info, query)
        result = select.query()

        if result:
            return result
        else:
            logging.error("Importação das get_import_last_rum_date deu pau")
            return False
    except Exception as e:
        logging.exception("An unexpected error occurred during BRANDS import" - e)
        return e


def get_coorp_conection_info(schema="public"):
    coorp_conection_info = {
        "host": os.environ.get("COORP_PGHOST"),
        "user": os.environ.get("COORP_PGUSER"),
        "port": 5432,
        "database": os.environ.get("COORP_PGDATABASE"),
        "password": os.environ.get("COORP_PGPASSWORD"),
        "schema": schema,
    }

    return coorp_conection_info


def get_data_conection_info(schema):
    data_conection_info = {
        "host": os.environ.get("DATA_PGHOST"),
        "user": os.environ.get("DATA_PGUSER"),
        "port": 5432,
        "database": os.environ.get("DATA_PGDATABASE"),
        "password": os.environ.get("DATA_PGPASSWORD"),
        "schema": schema,
    }

    return data_conection_info


def get_api_conection_info(integration_id):
    try:
        print(integration_id)

        data = integrationInfo(get_coorp_conection_info(integration_id), integration_id)

        api_conection_info = data[0][1][0]
        VTEX_API_AppKey = api_conection_info["vtex_api_appkey"]

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
        raise e
    finally:
        pass


def brands(integration_id):
    coorp_conection_info = get_coorp_conection_info(integration_id)
    data_conection_info = get_data_conection_info(integration_id)
    api_conection_info = get_api_conection_info(integration_id)

    from modules.vtex import brand

    try:
        brand.get_brands_list_parallel(api_conection_info, data_conection_info)
        return True
    except Exception as e:
        logging.exception(f"An unexpected error occurred during DAG - {e}")
        raise e
    finally:
        pass


def category(integration_id):
    coorp_conection_info = get_coorp_conection_info(integration_id)
    data_conection_info = get_data_conection_info(integration_id)
    api_conection_info = get_api_conection_info(integration_id)

    from modules.vtex import category_concurrent as category

    try:
        category.set_globals(30, api_conection_info, data_conection_info)
        return True
    except Exception as e:
        logging.exception(f"An unexpected error occurred during DAG - {e}")
        raise e
    finally:
        pass


def skus(integration_id):
    coorp_conection_info = get_coorp_conection_info(integration_id)
    data_conection_info = get_data_conection_info(integration_id)
    api_conection_info = get_api_conection_info(integration_id)

    from modules.vtex import sku

    try:
        sku.set_globals(1, api_conection_info, data_conection_info)
        return True
    except Exception as e:
        logging.exception(f"An unexpected error occurred during DAG - {e}")
        raise e
    finally:
        pass


def products(integration_id):
    coorp_conection_info = get_coorp_conection_info(integration_id)
    data_conection_info = get_data_conection_info(integration_id)
    api_conection_info = get_api_conection_info(integration_id)

    from modules.vtex import products

    try:
        products.set_globals(api_conection_info, data_conection_info)
        return True
    except Exception as e:
        logging.exception(f"An unexpected error occurred during DAG - {e}")
        raise e
    finally:
        pass


def orders_list(integration_id):
    api_conection_info = get_api_conection_info(integration_id)
    data_conection_info = get_data_conection_info(integration_id)
    coorp_conection_info = get_coorp_conection_info(integration_id)
    last_rum_date = get_import_last_rum_date(coorp_conection_info, integration_id)

    from modules.vtex import orders_list

    try:
        end_date = datetime.now()

        if last_rum_date[0][0][0] == None:
            start_date = end_date - timedelta(days=730)
        else:
            start_date = last_rum_date[0][0][0] - timedelta(days=1)

        orders_list.set_globals(
            api_conection_info,
            data_conection_info,
            coorp_conection_info,
            start_date=start_date,
            end_date=end_date,
        )

        return True
    except Exception as e:
        logging.exception(f"An unexpected error occurred during DAG - {e}")
        raise e
    finally:
        pass


def orders(integration_id):
    api_conection_info = get_api_conection_info(integration_id)
    data_conection_info = get_data_conection_info(integration_id)
    coorp_conection_info = get_coorp_conection_info(integration_id)

    from modules.vtex import orders

    try:
        orders.set_globals(api_conection_info, data_conection_info, coorp_conection_info)

        return True
    except Exception as e:
        logging.exception(f"An unexpected error occurred during DAG - {e}")
        raise e
    finally:
        pass


def orders_items(integration_id):
    api_conection_info = get_api_conection_info(integration_id)
    data_conection_info = get_data_conection_info(integration_id)
    coorp_conection_info = get_coorp_conection_info(integration_id)

    from modules.vtex import orders_items

    try:
        orders_items.set_globals(
            api_conection_info, data_conection_info, coorp_conection_info
        )

        return True
    except Exception as e:
        logging.exception(f"An unexpected error occurred during DAG - {e}")
        raise e
    finally:
        pass


def orders_totals(integration_id):
    api_conection_info = get_api_conection_info(integration_id)
    data_conection_info = get_data_conection_info(integration_id)
    coorp_conection_info = get_coorp_conection_info(integration_id)

    from modules.vtex import orders_totals

    try:
        orders_totals.set_globals(
            api_conection_info, data_conection_info, coorp_conection_info
        )

        return True
    except Exception as e:
        logging.exception(f"An unexpected error occurred during DAG - {e}")
        raise e
    finally:
        pass


def orders_shipping(integration_id):
    api_conection_info = get_api_conection_info(integration_id)
    data_conection_info = get_data_conection_info(integration_id)
    coorp_conection_info = get_coorp_conection_info(integration_id)

    from modules.vtex import orders_shipping

    try:
        orders_shipping.set_globals(
            api_conection_info, data_conection_info, coorp_conection_info
        )

        return True
    except Exception as e:
        logging.exception(f"An unexpected error occurred during DAG - {e}")
        raise e
    finally:
        pass


def client_profile(integration_id):
    api_conection_info = get_api_conection_info(integration_id)
    data_conection_info = get_data_conection_info(integration_id)
    coorp_conection_info = get_coorp_conection_info(integration_id)

    from modules.vtex import client_profile

    try:
        client_profile.set_globals(
            api_conection_info, data_conection_info, coorp_conection_info
        )

        return True
    except Exception as e:
        logging.exception(f"An unexpected error occurred during DAG - {e}")
        raise e
    finally:
        pass


def creteinfra2(integration_id):
    api_conection_info = get_api_conection_info(integration_id)
    data_conection_info = get_data_conection_info(integration_id)
    coorp_conection_info = get_coorp_conection_info(integration_id)

    from modules.vtex import client_profile

    try:
        client_profile.set_globals(
            api_conection_info, data_conection_info, coorp_conection_info
        )

        return True
    except Exception as e:
        logging.exception(f"An unexpected error occurred during DAG - {e}")
        raise e
    finally:
        pass


if __name__ == "__main__":
    integration_id = "c01838dc-fc3b-401c-91ea-fa8eaeee4ce6"

    # orders_items(integration_id)
    # orders_totals(integration_id)
    # orders_shipping(integration_id)
    # client_profile(integration_id)
    brands(integration_id)
