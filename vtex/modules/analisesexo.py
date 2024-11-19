import pandas as pd
from teste_dbpgconn import WriteJsonToPostgres
import logging
import numpy as np
from unidecode import unidecode
#instalar pip install pandas scikit-learn
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.naive_bayes import MultinomialNB
from sklearn.metrics import accuracy_score
from save_to_blobstorage import *


def treinar_modelo_e_avaliar():


    # Exemplo de dataset com nomes e gêneros
    df = pd.read_excel('arquivo_sexo.xlsx')
    df['nome_ajustsexo'] = df['first_name'].str.upper()
    df['nome_ajustsexo'] = df['nome_ajustsexo'].apply(lambda x: unidecode(x))
    df['ultimas_letras'] = df['nome_ajustsexo'].apply(lambda x: get_last_n_letters(x, n=2))      
    #df = pd.DataFrame(data)

    # Convertendo nomes em features
    vectorizer = CountVectorizer(analyzer='char')  # Analisador de caracteres
    X = vectorizer.fit_transform(df['ultimas_letras'])

    # Conjunto de dados de entrada e saída
    y = df['classification']

    # Dividindo o dataset em treino e teste
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

    # Treinando o modelo Naive Bayes
    model = MultinomialNB()
    model.fit(X_train, y_train)

    # Prevendo o gênero com base no nome
    y_pred = model.predict(X_test)

    # Exibindo acurácia do modelo
    accuracy = accuracy_score(y_test, y_pred)
    print(f'Acurácia: {accuracy * 100:.2f}%')
    
    
    return model, vectorizer


    # Função para prever o sexo com base no nome
def prever_sexo(nome, model, vectorizer):
    nome_transformado = vectorizer.transform([nome])
    predicao = model.predict(nome_transformado)
    return predicao[0]


  
def get_base_sexo_treino():
    
    try:
        execute_blob().get_file("appgemdata","teams-pictures/Logo_GD_preto.png","./relatorio_semanal/logo_"+celular+extensao) 
    
    except Exception as e:
        logging.error(f"erro ao pegar a base de sexo para treino {e}")
        raise  # Ensure the Airflow task fails on error



def getbase(data_conection_info,schema):
    try:
        query =f""" 
				select distinct userprofileid 
                ,split_part(ltrim(rtrim(firstname)), ' ', 1) as firstname  
                from "{schema}".client_profile cp 
                """
       
        _, result = WriteJsonToPostgres("integrations-pgserver-prod", query, "client_profile").query()
        if not result:
            logging.warning("No cliente_profile found in the database.")
            return False

        #print(result) 
        #df_tupla = pd.DataFrame(result, columns=['idprod', 'namesku', 'revenue_without_shipping', 'pedidos', 'revenue_orders', 'tickemedio', 'receita_incremental'])
        df_usuario = pd.DataFrame(result)
        #print (df_usuario)
        df_usuario['nome_comp'] = df_usuario['firstname'].str.upper()
        df_usuario['nome_ajust_bd'] = df_usuario['nome_comp'].apply(lambda x: unidecode(x))
        #ordenando do dataframe 
        df_base_comp = pd.read_excel('arquivo_sexo.xlsx')
        df_base_comp['nome_ajustsexo'] = df_base_comp['first_name'].str.upper()
       


        # Verificar se os valores da coluna 'id' de A estão no DataFrame B
        df_usuario['encontrado_em_b'] = df_usuario['nome_ajust_bd'].isin(df_base_comp['nome_ajustsexo'])

        df_usuario.to_excel("teste1.xlsx",index=False)


    except Exception as e:
        logging.error(f"An error occurred in get_categories_id_from_db: {e}")
        raise  # Ensure the Airflow task fails on error
    
# Função para pegar as últimas N letras de cada nome
def get_last_n_letters(nome, n=2):
    return nome[-n:]



model, vectorizer = treinar_modelo_e_avaliar()

# Exemplo de uso
nome = input("Digite um nome: ")
resultado = prever_sexo(nome,model, vectorizer)
print(f"O provável sexo para o nome {nome} é {resultado}.")


#algoritimo("integrations-pgserver-prod","2dd03eaf-cf56-4a5b-bc99-3a06b237ded8")    