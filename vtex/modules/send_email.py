from airflow.hooks.base import BaseHook
from smtplib import SMTP_SSL
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import os


def send_email_via_connection(emailnameairflow,listaemail_recebido,assunto,corpoemail,isexistfile,filename = None):


        #'report_email'
        # Obter a conexão cadastrada no Airflow
        connection = BaseHook.get_connection(emailnameairflow)  # Nome da sua conexão SMTP
        server =SMTP_SSL(host=connection.host, port=connection.port) 
                
        if not server:
            raise ValueError("Connection SMTP SLL do Email não está configurada corretamente.")
        # Define o conteúdo do e-mail
        msg= MIMEMultipart()
        msg['Subject'] = assunto
        msg['From'] = connection.login
        msg['To'] = listaemail_recebido
        # Envia o e-mail usando as configurações da conexão

            # Adiciona o corpo do e-mail
        body = MIMEText(corpoemail, 'html')
        msg.attach(body)

        if(isexistfile):
            
            try:
           
                attachment_path = filename
                if not os.path.exists(attachment_path):
                    raise FileNotFoundError(f"O arquivo {attachment_path} não foi encontrado.")
                # Adiciona o anexo
            # Coloque o caminho para o arquivo que deseja anexar
                filename = os.path.basename(attachment_path)
                with open(attachment_path, 'rb') as attachment_file:
                    # Cria a parte do anexo
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(attachment_file.read())
                    encoders.encode_base64(part)
                    part.add_header('Content-Disposition', f'attachment; filename={filename}')
                    msg.attach(part)
            except Exception as e:
                print(f"Erro ao enviar e-mail: {e}")
                raise 
            
        try:
            # Não use starttls() com SMTP_SSL, pois a conexão já é segura desde o início
            server.login(connection.login, connection.password)
            server.sendmail(msg['From'], [msg['To']], msg.as_string())
            print("E-mail enviado com sucesso!")
        except Exception as e:
            print(f"Erro ao enviar e-mail: {e}")
