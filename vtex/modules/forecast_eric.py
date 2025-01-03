

import pandas as pd
import logging
from datetime import datetime, timedelta
import numpy as np
import subprocess
import sys

from teste_dbpgconn import WriteJsonToPostgres

# Função para instalar um pacote via pip
def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

# Instalar matplotlib se não estiver instalado
try:
    from sklearn.model_selection import train_test_split
    from sklearn.ensemble import HistGradientBoostingRegressor
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.metrics import mean_squared_log_error
except ImportError:
    print("scikit-learn não está instalado. Instalando agora...")
    install("scikit-learn")
    from sklearn.model_selection import train_test_split
    from sklearn.ensemble import HistGradientBoostingRegressor
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.metrics import mean_squared_log_error

# Instalar matplotlib se não estiver instalado
try:
    from xgboost import XGBRegressor
except ImportError:
    print("xgboost não está instalado. Instalando agora...")
    install("xgboost")
    from xgboost import XGBRegressor


# Instalar matplotlib se não estiver instalado
# try:
#     from lightgbm import LGBMRegressor
# except ImportError:
#     print("lightgbm não está instalado. Instalando agora...")
#     install("lightgbm")
#     from lightgbm import LGBMRegressor


# Instalar matplotlib se não estiver instalado
try:
    from catboost import CatBoostRegressor
except ImportError:
    print("catboost não está instalado. Instalando agora...")
    install("catboost")
    from catboost import CatBoostRegressor

# Instalar matplotlib se não estiver instalado
try:
    from statsmodels.tsa.seasonal import seasonal_decompose
except ImportError:
    print("statsmodels não está instalado. Instalando agora...")
    install("statsmodels")
    from statsmodels.tsa.seasonal import seasonal_decompose



# # Variáveis globais
# api_conection_info = None
# data_conection_info = "integrations-data-dev"
# coorp_conection_info = None


#nção para calcular o RMSLE
def rmsle(y_true, y_pred):
    return np.sqrt(mean_squared_log_error(y_true + 1, y_pred + 1))  # Adiciona 1 para evitar log(0)


def CriaDataFrameFeriado(schema= "5e164a4b-5e09-4f43-9d81-a3d22b09a01b"):
    '''Funcao para realizar consulta no sql e gravar em dataframe do pandas'''
    try:
        query_feriado = f"""
                    select 
                        dt_feriado,
                        case 
                            when fl_feriado_ativo = True and nm_feriado <> 'Black friday' and nm_feriado <> 'Cyber monday' then 1 
                            else 0 
                        end as fl_feriado_ativo,
                        case
                            when nm_feriado = 'Black friday' then 1
                            else 0
                        end as fl_feriado_bf,
                        case 
                            when nm_feriado = 'Cyber monday' then 1
                            else 0
                        end as fl_feriado_cm    
                    from "{schema}".tb_forecast_feriado;
                        """
        _, feriado = WriteJsonToPostgres(data_conection_info, query_feriado, "tb_forecast_feriado").query()

        df_feriado = pd.DataFrame(feriado)
        df_feriado = df_feriado.rename(columns={'dt_feriado': 'dt_pedido'})
        df_feriado['dt_pedido'] = df_feriado['dt_pedido'].apply(lambda x: x.strftime('%Y-%m-%d'))
        df_feriado['dt_pedido'] = pd.to_datetime(df_feriado['dt_pedido'])
        # Return
        return df_feriado
    except Exception as e: 
        logging.error(f"An unexpected error occurred while processing the page: {e}")
        raise  # Ensure any error fails the Airflow task
        
 
def CriaDataFrameRealizado(schema):
    '''Funcao para realizar consulta no sql e gravar em dataframe do pandas'''
    try:
        query_realizado = f"""
                    select 
                        date_trunc('day',o.creationdate) as dt_pedido,
                        SUM(round(cast(o.revenue as numeric),2)) as sum_revenue, 
                        --count(1) as sum_revenue, 
                        
                        
                        'realizado' as nm_tipo_registro 
                    from "{schema}".orders_ia as o 
                    group by date_trunc('day',o.creationdate) order by 1 asc 
                            """
        # print(query_realizado)
        # print(data_conection_info)

        _, result = WriteJsonToPostgres(data_conection_info, query_realizado, "orders_ia").query()
        df_new = pd.DataFrame(result)
        # Return
        return df_new
    except Exception as e: 
        logging.error(f"An unexpected error occurred while processing the page: {e}")
        raise  # Ensure any error fails the Airflow task
        


def tratar_base(schema):
   
    try:
        # Cria dataframe com os dados historicos do faturamento realizado
        df_realizado = CriaDataFrameRealizado(schema)

        # Cria dataframe com as referencias de feriados passados e futuros (ate 2030)
        df_feriado = CriaDataFrameFeriado()
        
        df_realizado['sum_revenue'] = pd.to_numeric(df_realizado['sum_revenue'], errors='coerce')

        # Cria dataframe com o horizonte futuro de forecast
        #min_realizado = str(df_realizado['dt_pedido'].min())[:10]
        min_realizado = str(df_realizado['dt_pedido'].min())[:10]
        max_realizado = str(df_realizado['dt_pedido'].max())[:10]
        d1 = datetime.strptime(min_realizado,"%Y-%m-%d")
        d2 = datetime.strptime(max_realizado,"%Y-%m-%d")
        

        df_full = df_realizado.merge(df_feriado, on='dt_pedido', how='left')
    
        df_full['fl_feriado_ativo'] = df_full['fl_feriado_ativo'].apply(lambda x: 0 if (np.isnan(x) or x == 0) else 1)
        df_full['fl_feriado_bf'] = df_full['fl_feriado_bf'].apply(lambda x: 0 if (np.isnan(x) or x == 0) else 1)
        df_full['fl_feriado_cm'] = df_full['fl_feriado_cm'].apply(lambda x: 0 if (np.isnan(x) or x == 0) else 1)
        df_full['nm_tipo_registro'] = df_full['nm_tipo_registro'].fillna('forecast')
    
    
        df_full['fl_blackfriday'] = df_full['dt_pedido'].apply(lambda x: 1 if ((x.month == 11 and x.day >= 25) or (x.month == 12 and x.day <= 1)) else 0)


        # Supondo que 'df' seja o DataFrame com colunas 'data' e 'faturamento'
        df_full['dt_pedido2'] = pd.to_datetime(df_full['dt_pedido'])
        df_full.set_index('dt_pedido2', inplace=True)
    

        # 1. Defasagens (Lags)
        df_full['faturamento_lag1'] = df_full['sum_revenue'].shift(1)
        df_full['faturamento_lag2'] = df_full['sum_revenue'].shift(2)
        df_full['faturamento_lag3'] = df_full['sum_revenue'].shift(3)
        df_full['faturamento_lag4'] = df_full['sum_revenue'].shift(4)
        df_full['faturamento_lag5'] = df_full['sum_revenue'].shift(5)
        df_full['faturamento_lag6'] = df_full['sum_revenue'].shift(6)
        df_full['faturamento_lag6'] = df_full['sum_revenue'].shift(7)

        # 2. Médias Móveis
        df_full['media_movel_7dias'] = df_full['sum_revenue'].rolling(window=7).mean()
        df_full['media_movel_30dias'] = df_full['sum_revenue'].rolling(window=30).mean()

        # 3. Variação Percentual
        df_full['variacao_percentual'] = df_full['sum_revenue'].pct_change()

        # 4. Componentes de Tendência e Sazonalidade
        # Decomposição aditiva
        try:
            decomposicao = seasonal_decompose(df_full['sum_revenue'], model='additive', period=365)
            df_full['tendencia'] = decomposicao.trend
            df_full['sazonalidade'] = decomposicao.seasonal
        except ValueError as e:
            print(f"Erro ao realizar decomposição: {e}")
           # df_full['tendencia'] = None
           # df_full['sazonalidade'] = None

        # 5. Indicadores Temporais
        df_full['dia_da_semana'] = df_full.index.dayofweek
        df_full['mes'] = df_full.index.month
        df_full['dia_do_mes'] = df_full.index.day
        df_full['semana_do_ano'] = df_full.index.isocalendar().week
    # df_full["id_dia"] = df_full["dt_pedido"].apply(lambda x: x.day)
        
        df_full['sum_revenue_365d'] = df_full['sum_revenue'].shift(365)
        df_full['media_90d_ano_anterior'] = df_full['sum_revenue'].shift(365).rolling(window=90).mean()
        df_full['media_30d_ano_anterior'] = df_full['sum_revenue'].shift(365).rolling(window=30).mean()
        df_full['media_60d_ano_anterior'] = df_full['sum_revenue'].shift(365).rolling(window=60).mean()


        df_full_tratado = df_full.drop(columns=['nm_tipo_registro'])
    
        

    #  df_full_tratado.info()
        
        return df_full_tratado

    except Exception as e: 
        logging.error(f"An unexpected error occurred while processing the page: {e}")
        raise  # Ensure any error fails the Airflow task



def rmsle(y_true, y_pred):
    """Função para calcular o RMSLE"""
    return np.sqrt(mean_squared_log_error(y_true + 1, y_pred + 1))


def selecionar_melhor_modelo(X_train, y_train, X_test, y_test):
   
    try:
        """Treina diferentes modelos e seleciona o com menor RMSLE"""
        modelos = {
         #   'LGBMRegressor': LGBMRegressor(),
            'HistGradientBoostingRegressor': HistGradientBoostingRegressor(),
            'XGBRegressor': XGBRegressor(),
            'CatBoostRegressor': CatBoostRegressor(verbose=0),
            'RandomForest': RandomForestRegressor(random_state=42, n_estimators=100)
        }

        resultados = []

        for nome, modelo in modelos.items():
            modelo.fit(X_train, y_train)
            previsoes = modelo.predict(X_test)
            erro_rmsle = rmsle(y_test, previsoes)
            resultados.append({'modelo': nome, 'rmsle': erro_rmsle, 'modelo_treinado': modelo})
            print(f"{nome} - RMSLE: {erro_rmsle:.4f}")

        # Encontrar o modelo com o menor RMSLE
        melhor_modelo = min(resultados, key=lambda x: x['rmsle'])
        print(f"\nMelhor modelo: {melhor_modelo['modelo']} com RMSLE: {melhor_modelo['rmsle']:.4f}")

        return melhor_modelo['modelo_treinado']

    except Exception as e: 
        logging.error(f"An unexpected error occurred while processing the page: {e}")
        raise  # Ensure any error fails the Airflow task


def gerar_projecao_a_partir_de_data(data_inicio,schema):
    
    try:
        """Trata os dados, seleciona o melhor modelo e faz a projeção a partir de uma data específica"""
        # Trata a base e retorna o DataFrame tratado
        dias_projecao=90
        df = tratar_base(schema)
        df.set_index('dt_pedido', inplace=True)

        # Filtrar os dados históricos até a data anterior a data_inicio
        data_inicio_datetime = datetime.strptime(data_inicio, "%Y-%m-%d")
        df = df[df.index < data_inicio_datetime]

        # Preparar dados para treinamento
        X = df.drop(columns=['sum_revenue'])
        y = df['sum_revenue']

        # Dividir os dados em treino e teste
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)

        # Selecionar o melhor modelo
        melhor_modelo = selecionar_melhor_modelo(X_train, y_train, X_test, y_test)

        # Criar um DataFrame para as próximas datas, começando em data_inicio
        ultimos_dados = df.iloc[-dias_projecao:]  # Últimos dias usados como base
        futuras_datas = [data_inicio_datetime + timedelta(days=i) for i in range(dias_projecao)]
        df_futuro = pd.DataFrame(index=futuras_datas)

        # Gerar os mesmos atributos para as próximas datas
        for lag in range(1, 8):
            df_futuro[f'faturamento_lag{lag}'] = ultimos_dados['sum_revenue'].shift(lag).values[-dias_projecao:]

        df_futuro['media_movel_7dias'] = ultimos_dados['sum_revenue'].rolling(window=7).mean().values[-dias_projecao:]
        df_futuro['media_movel_30dias'] = ultimos_dados['sum_revenue'].rolling(window=30).mean().values[-dias_projecao:]

        # Usando weekday() para obter o dia da semana
        df_futuro['dia_da_semana'] = [d.weekday() for d in futuras_datas]
        df_futuro['mes'] = [d.month for d in futuras_datas]
        df_futuro['dia_do_mes'] = [d.day for d in futuras_datas]
        df_futuro['semana_do_ano'] = [d.isocalendar().week for d in futuras_datas]

        # Adicionar todas as colunas usadas no treinamento com valores padrão (zero)
        colunas_treinamento = X_train.columns
        for coluna in colunas_treinamento:
            if coluna not in df_futuro.columns:
                df_futuro[coluna] = 0

        # Garantir que as colunas estejam na mesma ordem
        df_futuro = df_futuro[colunas_treinamento]

        # Fazer previsões para as próximas datas com o melhor modelo
        previsoes = melhor_modelo.predict(df_futuro)

        # Criar um DataFrame com as datas e previsões
        df_resultado = pd.DataFrame({
            'creationdateforecast': futuras_datas,
            'predicted_revenue': previsoes
        })

        # Salvar os resultados em um CSV
        df_resultado.to_csv("forecast_a_partir_de_1_nov_24.csv", index=False)
        print("Projeção salva no arquivo 'forecast_a_partir_de_1_nov_24.csv'.")

        return df_resultado
    
    except Exception as e: 
        logging.error(f"An unexpected error occurred while processing the page: {e}")
        raise  # Ensure any error fails the Airflow task


def inserir_forecast(future_df,schema):

    try:
            
        final_df = future_df[['creationdateforecast', 'predicted_revenue']]

        final_df.loc[:, 'predicted_revenue'] = final_df['predicted_revenue'].round(2)
    
        query= f"""
                        DROP TABLE IF EXISTS "{schema}".orders_ia_forecast;
                        CREATE TABLE "{schema}".orders_ia_forecast (
                        creationdateforecast timestamp not NULL,
                        predicted_revenue numeric not NULL,
                        CONSTRAINT constraint_orders_forecast UNIQUE (creationdateforecast)
        );
                        
        """

        WriteJsonToPostgres(data_conection_info, query).execute_query_ddl()

        bath=final_df.to_dict(orient='records')

        WriteJsonToPostgres(data_conection_info, bath,"orders_ia_forecast","creationdateforecast").insert_data_batch(bath)
    except Exception as e: 
        logging.error(f"An unexpected error occurred while processing the page: {e}")
        raise  # Ensure any error fails the Airflow task


data_conection_info = "integrations-data-prod"
projecao = gerar_projecao_a_partir_de_data("2024-11-01","51d7b487-2586-4410-8b8b-d8569e68415f")


print(projecao)

#inserir_forecast(projecao,"51d7b487-2586-4410-8b8b-d8569e68415f")

