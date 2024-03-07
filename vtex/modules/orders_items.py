from vtex.modules.config import *
from vtex.modules.dbpgconn import *
from vtex.modules.api_conection import *
from vtex.modules.helpers import *

api_conection_info=None
data_conection_info=None
coorp_conection_info=None



def create_orders_items_database(table_name):
    try:
        writer = WriteJsonToPostgres('{}', table_name)

        if not writer.table_exists():
            # Construa a consulta dinâmica
            query = f"""SELECT orders.orderid, orders.{table_name} FROM orders LIMIT 1;"""

            result = WriteJsonToPostgres(query, table_name).query()
            logging.info(result)

            dados = result[0][1]

            for item in dados:
                item['orderid'] = result[0][0]

            logging.info(dados)

            writer = WriteJsonToPostgres(dados, table_name)
            writer.create_table()

            logging.info(f"create_orders_items_database - Tabela '{table_name}' criada com sucesso.")
            return True
        else:
            logging.info(f"create_orders_items_database - Tabela '{table_name}' já existe.")
            return True
    except Exception as e:
        logging.error(f"create_orders_items_database - Erro ao criar tabela - {e}")
        return e


def write_orders_item_to_database(batch_size=400):
    try:

        # Conte o número total de registros
        count_query = f"""SELECT count(*)
                        FROM orders
                        WHERE orders.orderid NOT IN (SELECT orderid FROM orders_items WHERE orderid IS NOT NULL)"""
        records = WriteJsonToPostgres(data_conection_info, count_query, 'orders_items').query()
        total_records = records[0][0][0]

        # Calcule o número total de lotes necessários
        total_batches = math.ceil(total_records / batch_size)

        # Itere sobre os lotes
        for batch_num in range(total_batches):
            # Construa a consulta dinâmica com a cláusula LIMIT para paginar os resultados
            offset = batch_num * batch_size
            query = f"""
            SELECT orders.orderid, orders.items
            FROM orders
            WHERE orders.orderid NOT IN (SELECT orderid FROM orders_items WHERE orderid IS NOT NULL)
            ORDER BY orders.sequence
            LIMIT {batch_size};
            """

            result = WriteJsonToPostgres(data_conection_info, query, 'orders_items').query()
            
            logging.info("query retornou")

        #     with concurrent.futures.ThreadPoolExecutor() as executor:
        #         futures = []
        #         for order_itens in result[0]:
        #             for id in order_itens[1]:
        #                 id['orderid'] = order_itens[0]
        #                 futures.append(executor.submit(insert_data_parallel, id, 'orders_items'))
        #         # Esperar que todas as tarefas paralelas sejam concluídas
        #         concurrent.futures.wait(futures)
                
        # with concurrent.futures.ThreadPoolExecutor() as executor:
        #     executor.map(write_orders_to_db, orders_ids[0])

            futures = []
            for order_itens in result[0]:
                for id in order_itens[1]:
                    id['orderid'] = order_itens[0]
                    futures.append(id)
                    
            with concurrent.futures.ThreadPoolExecutor() as executor:
                executor.map(insert_data_parallel, futures)

        return True

    except Exception as e:
        logging.error(f"write_orders_item_to_database - Erro desconhecido - {e}")
        return e


def insert_data_parallel(item):
    try:
        writer = WriteJsonToPostgres(data_conection_info, item, 'orders_items','uniqueid')
        writer.upsert_data()
        logging.info(f"Data upserted successfully for orderid - {item['orderid']}")
        return True
    except Exception as e:
        logging.error(f"insert_data_parallel - Erro ao inserir dados - {e}")
        return e
        


def set_globals(api_info, data_conection,coorp_conection, **kwargs):

    global api_conection_info
    api_conection_info = api_info
    
    global data_conection_info
    data_conection_info = data_conection
    
    global coorp_conection_info
    coorp_conection_info = coorp_conection
    
    write_orders_item_to_database()


if __name__ == "__main__":
    write_orders_item_to_database('orders_items')