import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


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



# Usando o decorator @dag para criar o objeto DAG
with DAG(
    "9-create-table-client",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["Createtabcliente", "v2", "all"],
    render_template_as_native_obj=True,
    params={
        "PGSCHEMA": Param(
            type="string",
            title="PGSCHEMA:",
            description="Enter the integration PGSCHEMA.",
            section="Important params",
            min_length=1,
            max_length=200,
        ),
        "ISDAILY": Param(
            type="boolean",
            title="ISDAILY:",
            description="Enter com False (processo total) ou True (processo diario) .",
            section="Important params",
            min_length=1,
            max_length=10,
        )
       
    },
) as dag:


    @task(provide_context=True)
    def tabelametaclient(**kwargs):
        PGSCHEMA = kwargs["params"]["PGSCHEMA"]
        try:
                  # Conexão com o banco de destino
                target_hook = PostgresHook(postgres_conn_id="integrations-pgserver-prod")

                        # Criar a tabela se não existir
                create_table_query = f"""
                    CREATE TABLE IF NOT EXISTS "{PGSCHEMA}".stg_teamgoal (
                        id bigint ,
                        year INT,
                        month INT,
                        goal NUMERIC,
                        integration_id UUID
                    );
                """
                target_hook.run(create_table_query)


                # Conecte-se ao PostgreSQL e execute o script
                hook = PostgresHook(postgres_conn_id="appgemdata-pgserver-prod")
                query = f"""
                                            
                    select 
                        tg.id, 
                        tg.year, 
                        tg.month, 
                        tg.goal , 
                        ii.id as integration_id 
                        from teams_teamgoal tg
                        inner join integrations_integration ii on 
                        ii.team_id = tg.team_id 
                        where 
                        ii.id ='{PGSCHEMA}'
                        and                    
                        ii.infra_create_status =  true 
                        and 
                        ii.is_active = true

                """
                dados_integration = hook.get_records(query)

                if not dados_integration:
                    return logging.exception(f"Sem dado de meta")  
                
                
              
                # Inserir os dados no banco de destino
                insert_query = f"""
                    TRUNCATE TABLE "{PGSCHEMA}".stg_teamgoal;
                    INSERT INTO "{PGSCHEMA}".stg_teamgoal (id, year, month, goal, integration_id)
                    VALUES (%s, %s, %s, %s, %s);
                """
                for row in dados_integration:
                    target_hook.run(insert_query, parameters=row)


        except Exception as e:
                logging.exception(f"Ocorreu um erro inesperado durante get_postgres_id - {e}")
                raise e



    @task(provide_context=True)
    def create_tabela_cliente_global(**kwargs):
        PGSCHEMA = kwargs["params"]["PGSCHEMA"]
        try:
            # Conecte-se ao PostgreSQL e execute o script
            hook = PostgresHook(postgres_conn_id="appgemdata-pgserver-prod")
            query = f"""
            select hosting from public.integrations_integration where id = '{PGSCHEMA}' 
 		    """
            hosting = hook.get_records(query)

        
        except Exception as e:
            logging.exception(
                f"An unexpected error occurred during create_tabela_global_cliente - {e}"
            )
            raise e
    
           

           # return [integration[0] for integration in integration_ids]

      

        logging.info(f"Processing params: {hosting[0][0]}")
        try:
            if hosting[0][0]== "vtex": 
                #essa parte é do vtex
                #esse schema usaremos para demo, para fazer videos e etc ..     
                #copiando do schema 2dd03
                if(PGSCHEMA == "5e164a4b-5e09-4f43-9d81-a3d22b09a01b"):
                    from modules.sqlscriptabglobaldemo import vtexsqlscriptscreatetabglobaldemo
                    sql_script = vtexsqlscriptscreatetabglobaldemo("5e164a4b-5e09-4f43-9d81-a3d22b09a01b")
                    
                else: # Defina o código SQL para criar a tabela
                    from modules.sqlscriptabglobal import vtexsqlscriptscreatetabglobal
                    sql_script = vtexsqlscriptscreatetabglobal(PGSCHEMA)    
                # Conecte-se ao PostgreSQL e execute o script
                # TODO postgres_conn_id deve ser uma variavel vinda da chamada da DAG
                # não pode estar cravada aqui no codigo
                hook = PostgresHook(postgres_conn_id="integrations-pgserver-prod")
                hook.run(sql_script)
                
                

            else:
                #essa parte é do shopify
                #copiando do schema 2dd03- probel 
                if(PGSCHEMA == "5e164a4b-5e09-4f43-9d81-a3d22b09a01b"):
                    from modules.sqlscriptabglobaldemo import vtexsqlscriptscreatetabglobaldemo
                    sql_script = vtexsqlscriptscreatetabglobaldemo("5e164a4b-5e09-4f43-9d81-a3d22b09a01b")
                    
                else: # Defina o código SQL para criar a tabela
                    from modules.sqlscriptabglobal import shopifysqlscriptscreatetabglobal
                    sql_script = shopifysqlscriptscreatetabglobal(PGSCHEMA)    
                # Conecte-se ao PostgreSQL e execute o script
                # TODO postgres_conn_id deve ser uma variavel vinda da chamada da DAG
                # não pode estar cravada aqui no codigo
                hook = PostgresHook(postgres_conn_id="integrations-pgserver-prod")
                hook.run(sql_script)

            return True

        except Exception as e:
            logging.exception(
                f"An unexpected error occurred during create_tabela_global_cliente - {e}"
            )
            raise e



    @task(provide_context=True)
    def tabelametaclientdia(**kwargs):
        PGSCHEMA = kwargs["params"]["PGSCHEMA"]
        try:
                  
            from modules.sqlscriptabglobal import globalsqlscriptsmeta
            sql_script = globalsqlscriptsmeta(PGSCHEMA)    
            hook3 = PostgresHook(postgres_conn_id="integrations-pgserver-prod")
            hook3.run(sql_script)

        except Exception as e:
                logging.exception(f"Ocorreu um erro inesperado na hora de criar a tabela meta cliente diaria - {e}")
                raise e



    trigger_dag_create_json = TriggerDagRunOperator(
        task_id="trigger_dag_create_json_dash",
        trigger_dag_id="9-forecast-revenue",  # Substitua pelo nome real da sua segunda DAG
        conf={
            "PGSCHEMA": "{{ params.PGSCHEMA }}",
            "ISDAILY":"{{ params.ISDAILY }}"
           
        },  # Se precisar passar informações adicionais para a DAG_B
    )

    # Configurando a dependência entre as tarefas

    try:
        create_tab_meta=tabelametaclient()
        create_tab_global_task = create_tabela_cliente_global()
        create_tab_meta_dia=tabelametaclientdia()


        create_tab_meta >> create_tab_global_task >> create_tab_meta_dia >>   trigger_dag_create_json
    
    except Exception as e:
        logging.error(f"Error inserting log diario: {e}")
    
        raise  # Ensure failure is propagated to Airflow