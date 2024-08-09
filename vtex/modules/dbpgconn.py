import json
import logging

# import openai
import psycopg2
from dotenv import load_dotenv


class PostgresConnection:
    def __init__(self, connection_info):
        load_dotenv()

        self.host = connection_info["host"]
        self.port = connection_info["port"]
        self.database = connection_info["database"]
        self.user = connection_info["user"]
        self.password = connection_info["password"]
        self.schema = connection_info["schema"]
        self.conn = None

    def connect(self):
        if self.conn is None:
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                options=f"-c search_path={self.schema}",
            )
            # Added to set the connection to use transactions
            self.conn.autocommit = False
        return self.conn

    def is_connected(self):
        return self.conn is not None

    def close(self):
        if self.conn is not None and not self.conn.closed:
            self.conn.close()
            self.conn = None

    def commit(self):
        if self.conn is not None and not self.conn.closed:
            self.conn.commit()

    def rollback(self):
        if self.conn is not None and not self.conn.closed:
            self.conn.rollback()


class WriteJsonToPostgres:
    def __init__(self, connection_info, data, tablename=None, table_key=None):
        self.connection = PostgresConnection(connection_info)
        self.data = data
        self.tablename = tablename
        self.table_key = table_key

    def _prepare_data(self, data):
        # Se o dado fornecido for um dicionário, converte para uma string JSON
        if isinstance(data, dict):
            data = json.dumps(data)
        return data

    def table_exists(self):
        try:
            with self.connection.connect() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        f"""
                        SELECT COUNT(*)
                        FROM pg_catalog.pg_tables
                        WHERE schemaname = 'public'
                        AND tablename = '{self.tablename}';
                        """
                    )

                    row = cursor.fetchone()

                    if row[0] > 0:
                        print(f"A tabela {self.tablename} já existe.")
                        return True
                    else:
                        print(f"A tabela {self.tablename} não existe.")
                        return False
        except Exception as e:
            print(f"Erro ao verificar a existência da tabela {self.tablename}: {e}")
            return False

    def query(self):
        try:
            logging.info(self.data)
            # cursor = self.connection.connect().cursor()
            # cursor.execute(self.data)
            # result = cursor.fetchall()
            # return result

            cursor = self.connection.connect().cursor()
            cursor.execute(self.data)

            # Obter os nomes das colunas
            column_names = [desc[0] for desc in cursor.description]

            # Obter os resultados com colunas nomeadas
            results = cursor.fetchall()
            results_named = [dict(zip(column_names, row)) for row in results]

            return results, results_named

        except Exception as e:
            # Em caso de falha, faça o rollback da transação e feche a conexão
            self.connection.rollback()

            # Caso o cursor já tenha sido criado, feche-o
            if "cursor" in locals():
                cursor.close()

            # Registro o erro para depuração
            logging.error(f"Erro ao Consultar os dados: {e}")

            return False  # Retorno em caso de falha

        finally:
            # Garantir que a conexão seja fechada mesmo se uma exceção ocorrer
            if self.connection:
                self.connection.close()

    # def generate_create_table(json_file_path, api_key, engine="text-davinci-003"):
    def generate_create_table(self):
        try:
            print("----------------------------------------")
            print("Tentando criar tabela")

            print(json.dumps(self.data))

            # Configuração da chave API da OpenAI
            openai.api_key = OPENAI_APIKEY

            # Verificar se a chave API foi configurada corretamente
            if openai.api_key is None:
                raise Exception("A chave API da OpenAI não foi definida.")

            messages = [
                {
                    "role": "user",
                    "content": f"""Crie uma instrução SQL com \
                                'CREATE TABLE IF NOT EXISTS' para PostgreSQL. \
                                Use o nome da tabela como '{self.tablename}'. \
                                Retorne somente a clausula solicitada. \\+
                                Nunca use o datatype JSONB[] prefira JSONB \
                                Defina as colunas e os tipos de dados baseando-se \
                                neste JSON: {json.dumps(self.data)}. \
                                """,
                },
                {"role": "assistant", "content": "CREATE TABLE IF NOT EXISTS "},
            ]

            print(messages)

            # Fazer a chamada para a API da OpenAI
            response = openai.chat.completions.create(
                model="gpt-3.5-turbo-16k",
                messages=messages,
                temperature=1,
                max_tokens=1000,
                top_p=1,
                frequency_penalty=0,
                presence_penalty=0,
            )

            # Construção da cláusula SQL CREATE TABLE
            sql_create = f"{response.choices[0].message.content}"
            print(sql_create)
            return sql_create
        except Exception as e:
            # Captura de exceções e exibição do erro
            print("Ocorreu um erro ao gerar a cláusula CREATE TABLE:", e)
            # raise e # Descomente esta linha para re-lançar a exceção
            return None

    def create_table(self):
        if not self.table_exists():
            try:
                # Execute some database operations
                with PostgresConnection() as db_connection:
                    cursor = db_connection.conn.cursor()
                    sql = f"CREATE TABLE IF NOT EXISTS {self.generate_create_table()}"

                    if sql:
                        print("Criando tabela")
                        cursor.execute(sql)
                        db_connection.commit()
                    else:
                        print("GPT não retornou resposta")
                        return False
            except Exception as e:
                # Rollback the changes in case of any error
                print("An error occurred, transaction rolled back:", e)
                return False

        return True

    def insert_data(self):
        try:
            cursor = self.connection.connect().cursor()

            # Extraia colunas e valores
            columns = self.data.keys()
            # values = [self.data[column] for column in columns]

            # Retorna os valore e se for dicionario retonar esse como string
            # values = [str(self.data[column]) if not isinstance(self.data[column], dict)
            # else str(self.data[column]) for column in columns]

            # values = [str(json.loads(self.data[column])) if
            # isinstance(self.data[column], dict) else str(self.data[column])
            # for column in columns]
            values = []
            for column in columns:
                if isinstance(self.data[column], (dict, list)):
                    value = str(json.dumps(self.data[column]))
                else:
                    value = self.data[column]

                values.append(value)

                # print (column, value, type(value))
            # print(values)

            # Crie uma string para os placeholders, por exemplo, (%s, %s, ...)
            placeholders = ", ".join(["%s"] * len(columns))

            # Prepare a instrução SQL com placeholders para os valores
            table_name = self.tablename  # Assume tablename é uma string
            sql_insert = "INSERT INTO {} ({}) VALUES ({})".format(
                table_name,
                ", ".join(
                    columns
                ),  # Certifique-se de que os nomes das colunas estão corretos
                placeholders,
            )

            # print(sql_insert)

            # Execute a instrução SQL com os valores reais
            cursor.execute(sql_insert, values)

            # Confirme a transação
            self.connection.commit()

            # Feche o cursor e a conexão
            cursor.close()
            # print(f"dados inseridos com sucesso: {values}")
            return True  # Retorno em caso de sucesso

        except Exception as e:
            # Em caso de falha, faça o rollback da transação e feche a conexão
            self.connection.rollback()

            # Caso o cursor já tenha sido criado, feche-o
            if "cursor" in locals():
                cursor.close()

            # Registro o erro para depuração
            logging.error(f"Erro ao inserir os dados: {e}")

            return False  # Retorno em caso de falha

        finally:
            # Garantir que a conexão seja fechada mesmo se uma exceção ocorrer
            if self.connection:
                self.connection.close()

    def upsert_data(self):
        try:
            # Use context manager to ensure cursor and connection are properly closed
            with self.connection.connect().cursor() as cursor:
                columns = self.data.keys()

                # Convert values to JSON for dictionary and list types
                data_values = [
                    json.dumps(value) if isinstance(value, (dict, list)) else value
                    for value in self.data.values()
                ]

                # Construct the UPSERT query using INSERT...ON CONFLICT
                upsert_query = f"""
                    INSERT INTO {self.tablename} ({', '.join(columns)})
                    VALUES %s
                    ON CONFLICT ({self.table_key}) DO UPDATE SET
                    ({', '.join(columns)}) = %s
                    RETURNING {self.table_key}
                """

                # Use mogrify to safely substitute the values into the query
                upsert_query = cursor.mogrify(
                    upsert_query, (tuple(data_values), tuple(data_values))
                )

                # print("Upsert Query:", upsert_query.decode())

                # Execute the UPSERT query and fetch the id
                cursor.execute(upsert_query)
                affected_id = cursor.fetchone()[0] if cursor.rowcount > 0 else None
                self.connection.commit()

                print(f"Data upserted successfully for {self.table_key}: {affected_id}")
                return True

        except Exception as e:
            # Handle exceptions and log errors
            self.connection.rollback()
            logging.error(f"Error during upsert operation: {e}")
            return None

    def insert_data_batch(self, batch_data):
        try:
            cursor = self.connection.connect().cursor()

            columns = list(batch_data[0].keys())

            # Construa uma string para os placeholders, por exemplo, (%s, %s, ...)
            placeholders = ", ".join(["%s"] * len(columns))

            # Construa a instrução SQL com os placeholders para os valores
            table_name = self.tablename
            sql_insert = "INSERT INTO {} ({}) VALUES ({})".format(
                table_name, ", ".join(columns), placeholders
            )

            # Construa uma lista de valores para todos os registros no lote
            values = []
            for data in batch_data:
                row_values = []
                for column in columns:
                    if isinstance(data[column], (dict, list)):
                        value = json.dumps(data[column])
                    else:
                        value = data[column]
                    row_values.append(value)
                values.append(tuple(row_values))

            # Execute a instrução SQL com os valores reais
            cursor.executemany(sql_insert, values)

            # Confirme a transação
            self.connection.commit()

            # Feche o cursor e a conexão
            cursor.close()
            print(f"Dados inseridos com sucesso: {len(batch_data)} registros")
            return True

        except Exception as e:
            # Em caso de falha, faça o rollback da transação e feche a conexão
            self.connection.rollback()

            # Caso o cursor já tenha sido criado, feche-o
            if "cursor" in locals():
                cursor.close()

            # Registre o erro para depuração
            print(f"Erro ao inserir os dados: {e}")

            return False

        finally:
            # Garanta que a conexão seja fechada mesmo se uma exceção ocorrer
            if self.connection:
                self.connection.close()


# if __name__ == "__main__":
#     with open(os.path.join(os.path.dirname(__file__), "client_profile_data.json"), "r") as f:
#         data = json.load(f)

#     writer = WriteJsonToPostgres(data, 'client_profile')
#     writer.create_table()
#     #writer.table_exists()
#     writer.insert_data()
