import logging
from datetime import datetime, time

from brand import get_brands_list_parallel
from category_concurrent import process_category_tree
from client_profile import write_client_profile_to_database
from dbpgconn import PostgresConnection, WriteJsonToPostgres
from orders import execute_process_orders_list, process_orders
from orders_items import write_orders_item_to_database
from orders_list import *
from orders_shipping import write_orders_shippingdata_to_database
from orders_totals import write_orders_totals_to_database_colunar
from products import process_products
from sku import get_skus


def integrationInfo(connection_info, integration_id):
    try:
        print("integrationInfo")

        start_time = time.time()

        postgres_conn = PostgresConnection(connection_info)

        query = f"""SELECT *
                    FROM public.integrations_integration
                    WHERE id = '{integration_id}';"""

        select = WriteJsonToPostgres(postgres_conn, query)
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
        logging.exception(f"An unexpected error occurred during BRANDS import - {e}")
        return False


def brands():
    # BRANDS - 1
    try:
        logging.info("BRANDS")
        start_time = time.time()
        result = get_brands_list_parallel()

        if result:
            logging.info(
                f"Importação das BRANDS Concluída com sucesso. \
                    Tempo de execução: {time.time() - start_time:.2f} segundos"
            )
            logging.info(f"Tempo de execução: {time.time() - start_time:.2f}")
            return True
        else:
            logging.error(
                f"Importação das BRANDS deu pau. \
                    Tempo de execução: {time.time() - start_time:.2f} segundos"
            )
            return False
    except Exception as e:
        logging.exception("An unexpected error occurred during BRANDS import" - e)
        return False


def categories():
    # CATEGORIES - 2
    try:
        logging.info("CATEGORIES")
        start_time = time.time()
        result = process_category_tree(30)

        if result:
            logging.info(
                f"Importação das CATEGORIES Concluída com sucesso.\
                    Tempo de execução: {time.time() - start_time:.2f} segundos"
            )
            return True
        else:
            logging.error(
                f"Importação das CATEGORIES deu pau. Tempo de execução:\
                    {time.time() - start_time:.2f} segundos"
            )
            return False
    except Exception as e:
        logging.exception("An unexpected error occurred during CATEGORIES import" - e)
        return False


def skus():
    # SKUS - 3
    try:
        logging.info("SKUS")
        start_time = time.time()
        result = get_skus(1)

        if result:
            logging.info(
                f"Importação dos SKUS Concluída com sucesso.\
                    Tempo de execução: {time.time() - start_time:.2f} segundos"
            )
            return True
        else:
            logging.error(
                f"Importação dos SKUS deu pau. Tempo de execução:\
                    {time.time() - start_time:.2f} segundos"
            )
            return False
    except Exception as e:
        logging.exception("An unexpected error occurred during SKUS import" - e)
        return False


def products():
    # PRODUCTS - 4
    try:
        logging.info("PRODUCTS")
        start_time = time.time()
        result = process_products()

        if result:
            logging.info(
                f"Importação dos PRODUCTS Concluída com sucesso. \
                    Tempo de execução: {time.time() - start_time:.2f} segundos"
            )
            return True
        else:
            logging.error(
                f"Importação dos PRODUCTS deu pau. Tempo de execução: \
                    {time.time() - start_time:.2f} segundos"
            )
    except Exception as e:
        logging.exception("An unexpected error occurred during PRODUCTS import" - e)
        return False


def orders_list(delta):
    # ORDENS LISTA - 5
    try:
        logging.info("ORDENS LISTA")
        end_date = datetime.now()
        # delta = 20
        result = execute_process_orders_list(end_date, delta)
        start_time = time.time()

        if result:
            logging.info(
                f"Importação dos dados da Lista de Ordens Concluída com sucesso. \
                    Tempo de execução: {time.time() - start_time:.2f} segundos"
            )
            return True
        else:
            logging.error(
                f"Importação dos dados da Lista de Ordens deu pau. Tempo de execução: \
                    {time.time() - start_time:.2f} segundos"
            )
    except Exception as e:
        logging.exception("An unexpected error occurred during ORDENS LISTA import" - e)
        return False


def orders():
    # ORDENS - 6
    try:
        logging.info("ORDENS")
        start_time = time.time()
        result = process_orders()

        if result:
            logging.info(
                f"Importação das ORDENS Concluída com sucesso. \
                    Tempo de execução: {time.time() - start_time:.2f} segundos"
            )
            return True
        else:
            logging.error(
                f"Importação das ORDENS deu pau. Tempo de execução: \
                    {time.time() - start_time:.2f} segundos"
            )
    except Exception as e:
        logging.exception("An unexpected error occurred during ORDENS import" - e)
        return False


def items():
    # ITENS DAS ORDENS - 7
    try:
        logging.info("ITENS DAS ORDENS")
        start_time = time.time()
        result = write_orders_item_to_database("orders_items")

        if result:
            logging.info(
                f"Importação dos ITENS DAS ORDENS Concluída com sucesso. \
                    Tempo de execução: {time.time() - start_time:.2f} segundos"
            )
            return True
        else:
            logging.error(
                f"Importação dos ITENS DAS ORDENS deu pau. Tempo de execução: \
                    {time.time() - start_time:.2f} segundos"
            )
    except Exception as e:
        logging.exception(
            "An unexpected error occurred during ITENS DAS ORDENS import" - e
        )
        return False


def orders_totals():
    # TOTAL DAS ORDENS - 8
    try:
        logging.info("TOTAL DAS ORDENS")
        start_time = time.time()
        result = write_orders_totals_to_database_colunar("orders_totals")

        if result:
            logging.info(
                f"Importação dos TOTAL DAS ORDENS Concluída com sucesso. \
                    Tempo de execução: {time.time() - start_time:.2f} segundos"
            )
            return True
        else:
            logging.error(
                f"Importação dos TOTAL DAS ORDENS deu pau. Tempo de execução: \
                    {time.time() - start_time:.2f} segundos"
            )
    except Exception as e:
        logging.exception(
            "An unexpected error occurred during TOTAL DAS ORDENS import" - e
        )
        return False


def orders_shipping():
    # ENVIO DAS ORDENS - 9
    try:
        logging.info("ENVIO DAS ORDENS")
        start_time = time.time()
        result = write_orders_shippingdata_to_database("orders_shippingdata")

        if result:
            logging.info(
                f"Importação dos ENVIO DAS ORDENS Concluída com sucesso. \
                    Tempo de execução: {time.time() - start_time:.2f} segundos"
            )
            return True
        else:
            logging.error(
                f"Importação dos ENVIO DAS ORDENS deu pau. Tempo de execução: \
                    {time.time() - start_time:.2f} segundos"
            )
    except Exception as e:
        logging.exception(
            "An unexpected error occurred during ENVIO DAS ORDENS import" - e
        )
        return False


def client_profile():
    # PROFILE DOS CLIENTES - 9
    try:
        logging.info("PROFILE DOS CLIENTES")
        start_time = time.time()
        result = write_client_profile_to_database("client_profile")

        if result:
            logging.info(
                f"Importação dos PROFILE DOS CLIENTES Concluída com sucesso. \
                    Tempo de execução: {time.time() - start_time:.2f} segundos"
            )
            return True
        else:
            logging.error(
                f"Importação dos PROFILE DOS CLIENTES deu pau. Tempo de execução: \
                    {time.time() - start_time:.2f} segundos"
            )
    except Exception as e:
        logging.exception(
            "An unexpected error occurred during PROFILE DOS CLIENTES import" - e
        )
        return False
