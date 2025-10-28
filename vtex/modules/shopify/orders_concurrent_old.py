import concurrent.futures
import http.client
import json
import logging
from datetime import datetime, timedelta
from urllib.parse import urlencode

from modules.dbpgconn import WriteJsonToPostgres
from modules.helpers import increment_one_day


def create_orders_database():
    try:
        query = "SELECT orderid FROM public.orders_list limit 1"
        writer = WriteJsonToPostgres(query, "orders")
        result = writer.query()
        print(result)

        if not writer.table_exists():
            try:
                qs = {"orderId": result[0][0]}
                orders = qs
                order_json = orders.get_order_by_id()
                print(order_json)

                writer = WriteJsonToPostgres(order_json, "orders")
                writer.create_table()
                print("create_orders_database - Tabela 'orders' CRIADA")
                return True
            except Exception as e:
                print(f"Erro desconhecido - {e}")
                return False
        else:
            print("create_orders_database - Tabela 'orders' já existe")
            return True

    except Exception as e:
        print(f"Erro desconhecido - {e}")
        return False


def create_orders_list_database(qs):
    qs = {"page": 1, "per_page": 5}
    listasd = get_orders_list_pages(qs)
    print(listasd)

    writer = WriteJsonToPostgres(listasd, "orders_list")
    writer.create_table()
    if not writer.table_exists():
        try:
            writer.create_table()
            print("create_orders_list_db - Tabela 'orders_list' CRIADA")
            return True
        except Exception as e:
            print(f"Erro desconhecido - {e}")
            return False
    else:
        print("create_orders_list_db - Tabela 'orders_list' já existe")
        return True


def get_orders_list_pages(qs):
    try:
        # Construa a string de consulta usando urlencode
        query_params = urlencode(qs)

        conn = http.client.HTTPSConnection(VTEX_Domain)

        conn.request("GET", f"/api/oms/pvt/orders?{query_params}", headers=headers)

        res = conn.getresponse()
        data = res.read()

        if res.status == 200:
            dados = json.loads(data.decode("utf-8"))
            if not dados:
                return None
            else:
                # print(data.decode("utf-8"))
                return dados
        else:
            print(f"Error: {res.status} - {res.reason}")
            return None

    except http.client.HTTPException as http_error:
        logging.error(f"HTTPException: {http_error}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return None
    finally:
        conn.close()


"""
orderId (string required)
"""


def get_order_by_id(orderId):
    try:
        # Construa a string de consulta usando urlencode
        query_params = orderId

        conn = http.client.HTTPSConnection(VTEX_Domain)

        conn.request("GET", f"/api/oms/pvt/orders/{query_params}", headers=headers)

        res = conn.getresponse()
        data = res.read()

        if res.status == 200:
            dados = json.loads(data.decode("utf-8"))
            if not dados:
                return None
            else:
                print(dados)
                return dados
        else:
            print(f"Error: {res.status} - {res.reason}")
            return None

    except http.client.HTTPException as http_error:
        logging.error(f"HTTPException: {http_error}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return None
    finally:
        if conn:
            conn.close()


def get_orders_ids_all(qs):
    try:
        qs = {"page": 1, "per_page": 100}
        listasd = get_orders_list_pages(qs)
        total_pages = listasd["paging"]["pages"]
        print(total_pages)

        values = []

        for page_number in range(total_pages):
            qs["page"] = page_number
            lista = get_orders_list_pages(qs)

            for obj in lista["list"]:
                values.append(obj["orderId"])

            print(values)

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")


def write_orders_list_db(qs):
    try:
        data_inicial = qs["start_date"]

        if not isinstance(data_inicial, datetime):
            try:
                logging.info("A variável é do tipo datetime.")
                data_inicial = datetime.strptime(data_inicial, "%Y-%m-%d")
            except Exception as e:
                logging.error(f"Não foi possível converter a data - {e}")
                return False

        logging.info("Data Inicial %s", data_inicial)

        data_final = qs["end_date"]

        if not isinstance(data_final, datetime):
            try:
                logging.info("A variável é do tipo datetime.")
                data_final = datetime.strptime(data_final, "%Y-%m-%d")
            except Exception as e:
                logging.error(f"Não foi possível converter a data - {e}")
                return False

        logging.info("Data Final %s", data_final)

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            while data_inicial <= data_final:
                start_date, end_date = increment_one_day(data_inicial)
                qs1 = {
                    "per_page": 100,
                    "f_creationDate": f"creationDate:[{start_date} TO {end_date}]",
                }
                logging.info(qs1)
                futures.append(executor.submit(process_page, qs1))
                data_inicial += timedelta(days=1)

            # Wait for all futures to complete
            for future in futures:
                future.result()

        return True

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return False


def process_page(qs):
    try:
        lista = get_orders_list_pages(qs)

        for obj in lista["list"]:
            logging.info(
                "----------------------------------------------------------------------"
            )
            logging.info(json.dumps(obj))

            try:
                writer = WriteJsonToPostgres(obj, "orders_list")
                writer.insert_data()
                logging.info("Created record -----------------------------------------")
            except Exception as e:
                logging.error(f"Error creating record - {e}")

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")


def write_orders_to_db():
    try:
        query = """SELECT DISTINCT orders_list.orderid
                    FROM orders_list
                    LEFT JOIN orders ON orders_list.orderid = orders.orderid
                    WHERE orders.orderid IS NULL;"""

        result = WriteJsonToPostgres(query, "orders")
        result = result.query()

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for order_id in result:
                future = executor.submit(process_order, order_id[0])
                futures.append(future)

            # Wait for all futures to complete
            for future in futures:
                future.result()

        return True

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return False


def process_order(order_id):
    try:
        order_json = get_order_by_id(order_id)

        try:
            writer = WriteJsonToPostgres(order_json, "orders")
            writer.insert_data()
            logging.info("Created record for order ID: %s", order_id)
        except Exception as e:
            logging.error(f"Error creating record - {e}")

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    # try:

    #     end_date =  datetime.now()
    #     start_date =  end_date - timedelta(days=730)

    #     qs_data = {"start_date": start_date, "end_date": end_date}

    #     if write_orders_list_db(qs_data):
    #         write_orders_to_db()

    # except Exception as e:
    #     logging.error(f"An unexpected error occurred: {e}")

    write_orders_to_db()

    # qs = {
    #     "page": 1,
    #     "per_page": 1
    #     }
    # orders = Orders(**qs)
    # orders.get_orders_list_pages()

    # qs = {
    #     "page": 1,
    #     "per_page": 1
    #     }
    # orders = Orders(**qs)
    # orders.create_orders_list_db()

    # qs = {
    #     "orderId": "1385390984848-01"
    #     }
    # orders = orders(**qs)
    # orders.get_order_by_id()

    # # # orders.get_orders_ids_all()

    # # # orders.create_orders_list_db()

    # orders.write_orders_list_db()

    # end_date =  datetime.now()
    # start_date =  end_date - timedelta(days=730)

    # qs_data = {"start_date": start_date, "end_date": end_date}
