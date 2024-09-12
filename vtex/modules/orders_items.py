import concurrent.futures
import logging
import math
from modules.dbpgconn import WriteJsonToPostgres

# Variáveis globais
api_conection_info = None
data_conection_info = None
coorp_conection_info = None

def create_orders_items_database(table_name):
    try:
        writer = WriteJsonToPostgres(data_conection_info, "{}", table_name)

        if not writer.table_exists():
            query = f"SELECT orders.orderid, orders.{table_name} FROM orders LIMIT 1;"
            result = writer.query()
            if not result or not result[0]:
                logging.error(f"No data found to create the table '{table_name}'.")
                raise ValueError(f"No data found to create the table '{table_name}'.")

            dados = result[0][1]
            for item in dados:
                item["orderid"] = result[0][0]

            writer = WriteJsonToPostgres(data_conection_info, dados, table_name)
            writer.create_table()

            logging.info(f"Table '{table_name}' created successfully.")
        else:
            logging.info(f"Table '{table_name}' already exists.")

    except Exception as e:
        logging.error(f"Error creating table '{table_name}': {e}")
        raise  # Rethrow the exception to ensure Airflow task failure

def write_orders_item_to_database(batch_size=400):
    try:
        count_query = """select COUNT(*) from orders o 
	                    where o.orderid in 
                        (select orderid from orders_list ol where ol.is_change = true )"""
        count_writer = WriteJsonToPostgres(data_conection_info, count_query, "orders_items")
        records = count_writer.query()
        
        if not records or not records[0]:
            logging.info("No records found to process.")
            return
        
        print(records)
        total_records = records[0][0][0]
        print(total_records)
        total_batches = math.ceil(total_records / batch_size)

        for batch_num in range(total_batches):
            offset = batch_num * batch_size
            query = f"""
                WITH max_data_insercao AS (
                    SELECT oi.orderid, MAX(oi.data_insercao) AS max_data_insercao
                    FROM orders_items oi
                    GROUP BY oi.orderid
                )
                SELECT o.orderid ,o.items
                FROM orders o
                INNER JOIN orders_list ol ON ol.orderid = o.orderid
                LEFT JOIN max_data_insercao mdi ON mdi.orderid = o.orderid
                WHERE ol.is_change = TRUE
                AND o.data_insercao > COALESCE(mdi.max_data_insercao, '1900-01-01')
                ORDER BY o.sequence
                LIMIT {batch_size};
            """
            batch_writer = WriteJsonToPostgres(data_conection_info, query, "orders_items")
            result = batch_writer.query()

            if not result or not result[0]:
                logging.info(f"No more data to process after batch {batch_num}.")
                break

            futures = []
            for order_itens in result[0]:
                for item in order_itens[1]:
                    item["orderid"] = order_itens[0]
                    futures.append(item)

            with concurrent.futures.ThreadPoolExecutor() as executor:
                future_to_item = {executor.submit(insert_data_parallel, item): item for item in futures}
                for future in concurrent.futures.as_completed(future_to_item):
                    item = future_to_item[future]
                    try:
                        future.result()
                    except Exception as e:
                        logging.error(f"Error processing orderid {item['orderid']}: {e}")
                        raise  # Propagate the exception to fail the Airflow task

    except Exception as e:
        logging.error(f"Unexpected error in write_orders_item_to_database: {e}")
        raise  # Ensure the Airflow task fails on error

def insert_data_parallel(item):
    try:
        writer = WriteJsonToPostgres(data_conection_info, item, "orders_items", "uniqueid")
        writer.upsert_data(isdatainsercao=1)
        logging.info(f"Data upserted successfully for orderid - {item['orderid']}")
    except Exception as e:
        logging.error(f"Error inserting data for orderid {item['orderid']}: {e}")
        raise  # Propagate the exception to fail the Airflow task

def set_globals(api_info, data_conection, coorp_conection, **kwargs):
    global api_conection_info, data_conection_info, coorp_conection_info
    api_conection_info = api_info
    data_conection_info = data_conection
    coorp_conection_info = coorp_conection

    if not all([api_conection_info, data_conection_info, coorp_conection_info]):
        logging.error("Global connection information is incomplete.")
        raise ValueError("All global connection information must be provided.")

    write_orders_item_to_database()

# if __name__ == "__main__":
#     set_globals(
#         {"api_key": "example"}, 
#         {"db_url": "postgresql://user:pass@localhost/db"}, 
#         {"coorp_key": "example"}
#     )
