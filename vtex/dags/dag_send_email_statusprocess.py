import logging

from datetime import datetime,timedelta
import os

from airflow import DAG
from airflow.decorators import task
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


with DAG(
    "000-send-email-StatusProcess",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["status", "v1", "reportstatus"],
     render_template_as_native_obj=True,
    params={
        
    },
) as dag:
    

    @task(provide_context=True)
    def get_statusprocess_integration():
        try:
           
            # Conecte-se ao PostgreSQL e execute o script
            hook = PostgresHook(postgres_conn_id="appgemdata-pgserver-prod")
            query = f"""
                            
                    SELECT  
                        id, 
                        "name",
                        hosting,
                        CASE
                            WHEN COALESCE(daily_run_date_ini, '1900-01-01'::timestamp) = '1900-01-01'::timestamp THEN 'ERRO - PROCESSO NOVO NÃO INICIADO'
                            WHEN daily_run_date_ini > COALESCE(daily_run_date_end, '1900-01-01'::timestamp) THEN 'ERRO - PROCESSO DIARIO'
                            WHEN daily_run_date_ini <= COALESCE(daily_run_date_end, '1900-01-01'::timestamp) THEN 'EXECUTADO COM SUCESSO'
                            ELSE 'ERRO - SEI LA'
                        END AS status_diario,
                        to_char(daily_run_date_ini,'DD-MM-YYYY HH24:MI') as data_ini,
                        to_char(daily_run_date_end,'DD-MM-YYYY HH24:MI')as data_fim
                    FROM integrations_integration ii
                    WHERE ii.is_active = TRUE 
                    AND ii.infra_create_status = TRUE;

            """
        
            status_daily = hook.get_records(query)
           # emails_string = ", ".join(emails_list)
            return status_daily
            
        except Exception as e:
            logging.exception(f"Erro ao achar o caminho do logo - {e}")
            raise

    @task(provide_context=True)
    def enviar_email(list_status):
        try:
            emails_string = 'gabriel.pereira.sousa@gmail.com'
            assunto = f"PROCESSO DIARIO - STATUS"

                    # Cabeçalho da tabela HTML
            tabela_html = """
            <p>Segue o status do processo diário:</p>
            <table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse;">
                <thead>
                    <tr>
                        <th>ID</th>
                        <th>Nome</th>
                        <th>Hosting</th>
                        <th>data_ini</th>
                        <th>data_fim</th>
                        <th>Status</th>
                    </tr>
                </thead>
                <tbody>
            """

            # Linhas da tabela
            for linha in list_status:
                status = linha[3]
                
                # Verifica se contém "erro" no texto (case insensitive)
                if "erro" in status.lower():
                    status_html = f'<td style="color: red; font-weight: bold;">{status}</td>'
                else:
                    status_html = f'<td>{status}</td>'

                tabela_html += f"""
                    <tr>
                        <td>{linha[0]}</td>
                        <td>{linha[1]}</td>
                        <td>{linha[3]}</td>
                        <td>{linha[4]}</td>
                        <td>{linha[2]}</td>
                        {status_html}
                    </tr>
                """

            # Fechamento da tabela
            tabela_html += """
                </tbody>
            </table>
            """

            corpo_email = tabela_html

            from modules import send_email
            send_email.send_email_via_connection(
                'tecnologia@gemdata.com.br',
                emails_string,
                assunto,
                corpo_email,
                False,
               # is_html=True  # <- importante informar que é HTML
            )

        except Exception as e:
            logging.exception(f"Erro ao enviar o e-mail - {e}")
            raise


    # Fluxo de e-mails
    status_daily = get_statusprocess_integration()
    post_email = enviar_email(status_daily)

    status_daily >> post_email