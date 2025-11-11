

import pandas as pd
import logging
from datetime import datetime, timedelta
import numpy as np
import subprocess
import sys

import calendar
from modules.dbpgconn import WriteJsonToPostgres

def install(package):
    '''Funcao para instalar os pacotes via pip'''  
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

# Instalar pacotes da lib sklearn se não estiver instalado
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

# Instalar lib math se não estiver instalado
try:
    import math
except ImportError:
    print("math não está instalado. Instalando agora...")
    install("math")
    import math

# # Instalar pacotes da lib lightgbm se não estiver instalado
# try:
#     from lightgbm import LGBMRegressor
# except ImportError:
#     print("lightgbm não está instalado. Instalando agora...")
#     install("lightgbm")
#     from lightgbm import LGBMRegressor

# Instalar pacotes da lib catboost se não estiver instalado
try:
    from catboost import CatBoostRegressor
except ImportError:
    print("catboost não está instalado. Instalando agora...")
    install("catboost")
    from catboost import CatBoostRegressor

# Instalar pacotes da lib statsmodels.tsa.seasonal se não estiver instalado
try:
    from statsmodels.tsa.seasonal import seasonal_decompose
except ImportError:
    print("statsmodels não está instalado. Instalando agora...")
    install("statsmodels")
    from statsmodels.tsa.seasonal import seasonal_decompose

# Instalar pacotes da lib xgboost se não estiver instalado
try:
    from xgboost import XGBRegressor
except ImportError:
    print("xgboost não está instalado. Instalando agora...")
    install("xgboost")
    from xgboost import XGBRegressor



# Variáveis globais
api_conection_info = None
data_conection_info = None
coorp_conection_info = None
date_start_info= None

def rmsle(y_true, y_pred):
    # Adiciona uma constante para garantir valores positivos
    y_true_adj = np.maximum(y_true, 0) + 1
    y_pred_adj = np.maximum(y_pred, 0) + 1
    return np.sqrt(mean_squared_log_error(y_true_adj, y_pred_adj))


def CriaDataFrameFeriado(schema= "5e164a4b-5e09-4f43-9d81-a3d22b09a01b"):
    '''Funcao para realizar consulta do calendario de feriados no sql e gravar em dataframe do pandas'''
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
                        end as fl_feriado_cm,
                         case
                            when nm_feriado = 'Natal' then 1
                            else 0
                        end as fl_feriado_nt    
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

def CriaDataFrameRealizado():
    '''Funcao para realizar consulta dos dados historicos realizados no sql e gravar em dataframe do pandas'''
    try:
        query_realizado = f"""
                    select 
                        date_trunc('day',o.creationdate) as dt_pedido,
                        SUM(round(cast(o.revenue as numeric),2)) as sum_revenue, 
                        --count(1) as sum_revenue, 
                        'realizado' as nm_tipo_registro 
                    from orders_ia as o 
                    where date_trunc('day',o.creationdate)  < CURRENT_DATE
                    group by date_trunc('day',o.creationdate) order by 1 asc 
                        """
        _, realizado = WriteJsonToPostgres(data_conection_info, query_realizado, "orders_ia").query()
        df_realizado = pd.DataFrame(realizado)
        df_realizado['dt_pedido'] = df_realizado['dt_pedido'].apply(lambda x: x.strftime('%Y-%m-%d'))
        df_realizado['dt_pedido'] = pd.to_datetime(df_realizado['dt_pedido'])
        # Return
        return df_realizado
    except Exception as e: 
        logging.error(f"An unexpected error occurred while processing the page: {e}")
        raise  # Ensure any error fails the Airflow task

def TratarBase():
    '''Funcao para realizar consulta no sql e gravar em dataframe do pandas'''
    try:
        # Cria dataframe com os dados historicos do faturamento realizado
        df_realizado = CriaDataFrameRealizado()
        # df_realizado.to_csv("df_realizado.csv", index=False)

        if(len(df_realizado) >= 365):

            # Cria dataframe com as referencias de feriados passados e futuros (ate 2030)
            df_feriado = CriaDataFrameFeriado()
            # df_feriado.to_csv("df_feriado.csv", index=False)

            df_realizado['sum_revenue'] = pd.to_numeric(df_realizado['sum_revenue'], errors='coerce')
            # df_realizado.to_csv("df_realizado_2.csv", index=False)

            # Cria dataframe com o horizonte futuro de forecast
            #min_realizado = str(df_realizado['dt_pedido'].min())[:10]
            min_realizado = str(df_realizado['dt_pedido'].min())[:10]
            max_realizado = str(df_realizado['dt_pedido'].max())[:10]
            d1 = datetime.strptime(min_realizado,"%Y-%m-%d")
            d2 = datetime.strptime(max_realizado,"%Y-%m-%d")
            print(d1)
            print(d2)

            df_full = df_realizado.merge(df_feriado, on='dt_pedido', how='left')
        
            df_full['fl_feriado_ativo'] = df_full['fl_feriado_ativo'].apply(lambda x: 0 if (np.isnan(x) or x == 0) else 1)
            df_full['fl_feriado_bf'] = df_full['fl_feriado_bf'].apply(lambda x: 0 if (np.isnan(x) or x == 0) else 1)
            df_full['fl_feriado_cm'] = df_full['fl_feriado_cm'].apply(lambda x: 0 if (np.isnan(x) or x == 0) else 1)
            df_full['fl_feriado_nt'] = df_full['fl_feriado_nt'].apply(lambda x: 0 if (np.isnan(x) or x == 0) else 1)
            df_full['nm_tipo_registro'] = df_full['nm_tipo_registro'].fillna('forecast')
        
            #df_full['fl_blackfriday'] = df_full['dt_pedido'].apply(lambda x: 1 if ((x.month == 11 and x.day >= 25) or (x.month == 12 and x.day <= 1)) else 0)

            # Supondo que 'df' seja o DataFrame com colunas 'data' e 'faturamento'
            df_full['dt_pedido2'] = pd.to_datetime(df_full['dt_pedido'])
            df_full.set_index('dt_pedido2', inplace=True)
        
            # 1. Defasagens do faturamento (Lags)
            df_full['faturamento_lag1'] = df_full['sum_revenue'].shift(1)
            df_full['faturamento_lag2'] = df_full['sum_revenue'].shift(2)
            df_full['faturamento_lag3'] = df_full['sum_revenue'].shift(3)
            df_full['faturamento_lag4'] = df_full['sum_revenue'].shift(4)
            df_full['faturamento_lag5'] = df_full['sum_revenue'].shift(5)
            df_full['faturamento_lag6'] = df_full['sum_revenue'].shift(6)
            df_full['faturamento_lag7'] = df_full['sum_revenue'].shift(7)

            # 2. Médias Móveis
            df_full['media_movel_7dias'] = df_full['sum_revenue'].rolling(window=7).mean().apply(lambda x: round(x, 2))
            df_full['media_movel_30dias'] = df_full['sum_revenue'].rolling(window=30).mean().apply(lambda x: round(x, 2))
        
            # 3. Variação Percentual
            #df_full['variacao_percentual'] = df_full['sum_revenue'].pct_change().apply(lambda x: round(x, 4))

            # 4. Componentes de Tendência e Sazonalidade
            # Decomposição aditiva
            #try:
            #    decomposicao = seasonal_decompose(df_full['sum_revenue'], model='additive', period=365)
            #    df_full['tendencia'] = decomposicao.trend.apply(lambda x: round(x, 2))
            #    df_full['sazonalidade'] = decomposicao.seasonal.apply(lambda x: round(x, 2))
            #except ValueError as e:
            #    print(f"Erro ao realizar decomposição: {e}")
            #   # df_full['tendencia'] = None
            #   # df_full['sazonalidade'] = None

            # 5. Cria as defasagens dos feriados normais
            df_full['fl_feriado_shift_1'] = df_full['fl_feriado_ativo'].shift(-1)
            df_full['fl_feriado_shift_1'] = (df_full['fl_feriado_shift_1'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)

            df_full['fl_feriado_shift_2'] = df_full['fl_feriado_ativo'].shift(-2)
            df_full['fl_feriado_shift_2'] = (df_full['fl_feriado_shift_2'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)

            df_full['fl_feriado_shift_3'] = df_full['fl_feriado_ativo'].shift(-3)
            df_full['fl_feriado_shift_3'] = (df_full['fl_feriado_shift_3'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)

            df_full['fl_feriado_shift_4'] = df_full['fl_feriado_ativo'].shift(-4)
            df_full['fl_feriado_shift_4'] = (df_full['fl_feriado_shift_4'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)

            df_full['fl_feriado_shift_5'] = df_full['fl_feriado_ativo'].shift(-5)
            df_full['fl_feriado_shift_5'] = (df_full['fl_feriado_shift_5'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)

            df_full['fl_feriado_shift_6'] = df_full['fl_feriado_ativo'].shift(-6)
            df_full['fl_feriado_shift_6'] = (df_full['fl_feriado_shift_6'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)

            df_full['fl_feriado_shift_7'] = df_full['fl_feriado_ativo'].shift(-7)
            df_full['fl_feriado_shift_7'] = (df_full['fl_feriado_shift_7'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)
        
            # 6. Cria as defasagens dos feriados black friday, cyber monday e natal
            df_full['fl_bf_shift_p1'] = df_full['fl_feriado_bf'].shift(+1)
            df_full['fl_bf_shift_p1'] = (df_full['fl_bf_shift_p1'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)

            df_full['fl_bf_shift_p2'] = df_full['fl_feriado_bf'].shift(+2)
            df_full['fl_bf_shift_p2'] = (df_full['fl_bf_shift_p2'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)

            df_full['fl_bf_shift_p3'] = df_full['fl_feriado_bf'].shift(+3)
            df_full['fl_bf_shift_p3'] = (df_full['fl_bf_shift_p3'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)

            df_full['fl_bf_shift_p4'] = df_full['fl_feriado_bf'].shift(+4)
            df_full['fl_bf_shift_p4'] = (df_full['fl_bf_shift_p4'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)

            df_full['fl_bf_shift_p5'] = df_full['fl_feriado_bf'].shift(+5)
            df_full['fl_bf_shift_p5'] = (df_full['fl_bf_shift_p5'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)

            df_full['fl_bf_shift_p6'] = df_full['fl_feriado_bf'].shift(+6)
            df_full['fl_bf_shift_p6'] = (df_full['fl_bf_shift_p6'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)

            df_full['fl_bf_shift_p7'] = df_full['fl_feriado_bf'].shift(+7)
            df_full['fl_bf_shift_p7'] = (df_full['fl_bf_shift_p7'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)

            df_full['fl_cm_shift_1'] = df_full['fl_feriado_cm'].shift(-1)
            df_full['fl_cm_shift_1'] = (df_full['fl_cm_shift_1'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)

            df_full['fl_cm_shift_2'] = df_full['fl_feriado_cm'].shift(-2)
            df_full['fl_cm_shift_2'] = (df_full['fl_cm_shift_2'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)

            df_full['fl_cm_shift_3'] = df_full['fl_feriado_cm'].shift(-3)
            df_full['fl_cm_shift_3'] = (df_full['fl_cm_shift_3'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)
            
            df_full['fl_nt_shift_1'] = df_full['fl_feriado_nt'].shift(-1)
            df_full['fl_nt_shift_1'] = (df_full['fl_nt_shift_1'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)
            
            df_full['fl_nt_shift_2'] = df_full['fl_feriado_nt'].shift(-2)
            df_full['fl_nt_shift_2'] = (df_full['fl_nt_shift_2'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)

            df_full['fl_nt_shift_3'] = df_full['fl_feriado_nt'].shift(-3)
            df_full['fl_nt_shift_3'] = (df_full['fl_nt_shift_3'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)

            df_full['fl_nt_shift_4'] = df_full['fl_feriado_nt'].shift(-4)
            df_full['fl_nt_shift_4'] = (df_full['fl_nt_shift_4'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)

            df_full['fl_nt_shift_5'] = df_full['fl_feriado_nt'].shift(-5)
            df_full['fl_nt_shift_5'] = (df_full['fl_nt_shift_5'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)
                                                    
            df_full['fl_nt_shift_6'] = df_full['fl_feriado_nt'].shift(-6)
            df_full['fl_nt_shift_6'] = (df_full['fl_nt_shift_6'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)

            df_full['fl_nt_shift_7'] = df_full['fl_feriado_nt'].shift(-7)
            df_full['fl_nt_shift_7'] = (df_full['fl_nt_shift_7'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)

            # 7. Cria variavel do dia da semana, dia, mes, ano, semana do mes
            df_full["id_dds"] = df_full["dt_pedido"].apply(lambda x:int(x.weekday()) + 2 if int(x.weekday()) + 2 != 8 else 1)
            df_full["id_dia"] = df_full["dt_pedido"].apply(lambda x: x.day)
            df_full["id_semana_do_mes"] = df_full["dt_pedido"].apply(lambda x: math.ceil(x.day/7))
            df_full["id_mes"] = df_full["dt_pedido"].apply(lambda x: x.month)
            df_full["id_ano"] = df_full["dt_pedido"].apply(lambda x: x.year)
            df_full["id_semana_do_ano"] = df_full["dt_pedido"].dt.isocalendar().week

            # 8. Cria variaveis dummys para cada dia da semana
            df_full["fl_domingo"] = 0
            df_full["fl_segunda"] = 0    
            df_full["fl_terca"] = 0
            df_full["fl_quarta"] = 0
            df_full["fl_quinta"] = 0
            df_full["fl_sexta"] = 0
            df_full["fl_sabado"] = 0
            df_full.loc[(df_full["id_dds"] == 1,"fl_domingo")] = 1
            df_full.loc[(df_full["id_dds"] == 2,"fl_segunda")] = 1
            df_full.loc[(df_full["id_dds"] == 3,"fl_terca")] = 1
            df_full.loc[(df_full["id_dds"] == 4,"fl_quarta")] = 1
            df_full.loc[(df_full["id_dds"] == 5,"fl_quinta")] = 1
            df_full.loc[(df_full["id_dds"] == 6,"fl_sexta")] = 1
            df_full.loc[(df_full["id_dds"] == 7,"fl_sabado")] = 1

            # 9. Cria variaveis dummys para cada semana dentro do mesmo mes
            df_full["fl_sem2_mes"] = 0
            df_full["fl_sem3_mes"] = 0    
            df_full["fl_sem4_mes"] = 0
            df_full["fl_sem5_mes"] = 0
            df_full.loc[(df_full["id_semana_do_mes"] == 2,"fl_sem2_mes")] = 1
            df_full.loc[(df_full["id_semana_do_mes"] == 3,"fl_sem3_mes")] = 1
            df_full.loc[(df_full["id_semana_do_mes"] == 4,"fl_sem4_mes")] = 1
            df_full.loc[(df_full["id_semana_do_mes"] == 5,"fl_sem5_mes")] = 1

            # 10. Cria variaveis dummys para cada mes
            df_full["fl_jan"] = 0
            df_full["fl_fev"] = 0
            df_full["fl_mar"] = 0
            df_full["fl_abr"] = 0
            df_full["fl_mai"] = 0
            df_full["fl_jun"] = 0
            df_full["fl_jul"] = 0
            df_full["fl_ago"] = 0
            df_full["fl_set"] = 0
            df_full["fl_out"] = 0
            df_full["fl_nov"] = 0
            df_full["fl_dez"] = 0
            df_full.loc[(df_full["id_mes"] == 1,"fl_jan")] = 1
            df_full.loc[(df_full["id_mes"] == 2,"fl_fev")] = 1
            df_full.loc[(df_full["id_mes"] == 3,"fl_mar")] = 1
            df_full.loc[(df_full["id_mes"] == 4,"fl_abr")] = 1
            df_full.loc[(df_full["id_mes"] == 5,"fl_mai")] = 1
            df_full.loc[(df_full["id_mes"] == 6,"fl_jun")] = 1
            df_full.loc[(df_full["id_mes"] == 7,"fl_jul")] = 1
            df_full.loc[(df_full["id_mes"] == 8,"fl_ago")] = 1
            df_full.loc[(df_full["id_mes"] == 9,"fl_set")] = 1
            df_full.loc[(df_full["id_mes"] == 10,"fl_out")] = 1
            df_full.loc[(df_full["id_mes"] == 11,"fl_nov")] = 1
            df_full.loc[(df_full["id_mes"] == 12,"fl_dez")] = 1

            # 11. Cria variaveis de media movel
            #df_full['sum_revenue_365d'] = df_full['sum_revenue'].shift(365)
            #df_full['media_90d_ano_anterior'] = df_full['sum_revenue'].shift(365).rolling(window=90).mean().apply(lambda x: round(x, 2))
            #df_full['media_30d_ano_anterior'] = df_full['sum_revenue'].shift(365).rolling(window=30).mean().apply(lambda x: round(x, 2))
            #df_full['media_60d_ano_anterior'] = df_full['sum_revenue'].shift(365).rolling(window=60).mean().apply(lambda x: round(x, 2))

            df_full_tratado = df_full.drop(columns=['nm_tipo_registro','id_dds','id_dia','id_mes','id_ano','fl_domingo','fl_jan','id_semana_do_ano'])
            # df_full_tratado.to_csv("df_full_tratado.csv", index=False)

            return df_full_tratado
        else:
            return df_realizado

    except Exception as e: 
        logging.error(f"An unexpected error occurred while processing the page: {e}")
        raise  # Ensure any error fails the Airflow task


def SelecionarMelhorModelo(X_train, y_train, X_test, y_test):
    """Treina diferentes modelos e seleciona o com menor RMSLE"""
    try: 
        modelos = {
            #'LGBMRegressor': LGBMRegressor(),
            'HistGradientBoostingRegressor': HistGradientBoostingRegressor(random_state=42),
            'XGBRegressor': XGBRegressor(random_state=42, verbosity=0),
            'CatBoostRegressor': CatBoostRegressor(verbose=0, random_state=42),
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


  
def gerar_projecao_a_partir_de_data(data_inicio):
    """Trata os dados, seleciona o melhor modelo e faz a projeção a partir de uma data específica"""   
    try:
        # Trata a base e retorna o DataFrame tratado
        data_inicio_datetime = datetime.strptime(data_inicio, "%Y-%m-%d")
        data_futura = data_inicio_datetime + timedelta(days=60)
        ultimo_dia = calendar.monthrange(data_futura.year, data_futura.month)[1]

        if data_futura.day != ultimo_dia:
            # Se não for o último dia, ajustar para o último
            data_futura = data_futura.replace(day=ultimo_dia)

        dias_projecao = (data_futura - data_inicio_datetime).days +1
        df = TratarBase()
        if(len(df) >= 365):     
            
            df.set_index('dt_pedido', inplace=True)

            # Filtrar os dados históricos até a data anterior a data_inicio
           
            df = df[df.index < data_inicio_datetime]

            # Preparar dados para treinamento
            X = df.drop(columns=['sum_revenue'])
            y = df['sum_revenue']

            # Dividir os dados em treino e teste
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)

            # Selecionar o melhor modelo
            melhor_modelo = SelecionarMelhorModelo(X_train, y_train, X_test, y_test)

            # Criar um DataFrame para as próximas datas, começando em data_inicio
            ultimos_dados = df.iloc[-dias_projecao:]  # Últimos dias usados como base
            futuras_datas = [data_inicio_datetime + timedelta(days=i) for i in range(dias_projecao)]
            df_futuro = pd.DataFrame(index=futuras_datas)
            df_futuro['dt_pedido'] = pd.to_datetime(df_futuro.index.values)
            
            # Cria dataframe com as referencias de feriados passados e futuros (ate 2030)
            df_feriado = CriaDataFrameFeriado()
            # df_feriado.to_csv("df_feriado.csv", index=False)

            df_futuro = df_futuro.merge(df_feriado, on='dt_pedido', how='left')

            df_futuro['fl_feriado_ativo'] = df_futuro['fl_feriado_ativo'].apply(lambda x: 0 if (np.isnan(x) or x == 0) else 1)
            df_futuro['fl_feriado_bf'] = df_futuro['fl_feriado_bf'].apply(lambda x: 0 if (np.isnan(x) or x == 0) else 1)
            df_futuro['fl_feriado_cm'] = df_futuro['fl_feriado_cm'].apply(lambda x: 0 if (np.isnan(x) or x == 0) else 1)
            df_futuro['fl_feriado_nt'] = df_futuro['fl_feriado_nt'].apply(lambda x: 0 if (np.isnan(x) or x == 0) else 1)

            # Gerar os mesmos atributos para as próximas datas
            for lag in range(1, 8):
                df_futuro[f'faturamento_lag{lag}'] = ultimos_dados['sum_revenue'].shift(lag).values[-dias_projecao:]

            df_futuro['media_movel_7dias'] = ultimos_dados['sum_revenue'].rolling(window=7).mean().values[-dias_projecao:]
            df_futuro['media_movel_30dias'] = ultimos_dados['sum_revenue'].rolling(window=30).mean().values[-dias_projecao:]

            # 5. Cria as defasagens dos feriados normais
            df_futuro['fl_feriado_shift_1'] = df_futuro['fl_feriado_ativo'].shift(-1)
            df_futuro['fl_feriado_shift_1'] = (df_futuro['fl_feriado_shift_1'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)

            df_futuro['fl_feriado_shift_2'] = df_futuro['fl_feriado_ativo'].shift(-2)
            df_futuro['fl_feriado_shift_2'] = (df_futuro['fl_feriado_shift_2'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)

            df_futuro['fl_feriado_shift_3'] = df_futuro['fl_feriado_ativo'].shift(-3)
            df_futuro['fl_feriado_shift_3'] = (df_futuro['fl_feriado_shift_3'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)

            df_futuro['fl_feriado_shift_4'] = df_futuro['fl_feriado_ativo'].shift(-4)
            df_futuro['fl_feriado_shift_4'] = (df_futuro['fl_feriado_shift_4'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)

            df_futuro['fl_feriado_shift_5'] = df_futuro['fl_feriado_ativo'].shift(-5)
            df_futuro['fl_feriado_shift_5'] = (df_futuro['fl_feriado_shift_5'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)

            df_futuro['fl_feriado_shift_6'] = df_futuro['fl_feriado_ativo'].shift(-6)
            df_futuro['fl_feriado_shift_6'] = (df_futuro['fl_feriado_shift_6'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)

            df_futuro['fl_feriado_shift_7'] = df_futuro['fl_feriado_ativo'].shift(-7)
            df_futuro['fl_feriado_shift_7'] = (df_futuro['fl_feriado_shift_7'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)

            # 6. Cria as defasagens dos feriados black friday, cyber monday e natal
            df_futuro['fl_bf_shift_p1'] = df_futuro['fl_feriado_bf'].shift(+1)
            df_futuro['fl_bf_shift_p1'] = (df_futuro['fl_bf_shift_p1'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)

            df_futuro['fl_bf_shift_p2'] = df_futuro['fl_feriado_bf'].shift(+2)
            df_futuro['fl_bf_shift_p2'] = (df_futuro['fl_bf_shift_p2'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)

            df_futuro['fl_bf_shift_p3'] = df_futuro['fl_feriado_bf'].shift(+3)
            df_futuro['fl_bf_shift_p3'] = (df_futuro['fl_bf_shift_p3'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)

            df_futuro['fl_bf_shift_p4'] = df_futuro['fl_feriado_bf'].shift(+4)
            df_futuro['fl_bf_shift_p4'] = (df_futuro['fl_bf_shift_p4'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)

            df_futuro['fl_bf_shift_p5'] = df_futuro['fl_feriado_bf'].shift(+5)
            df_futuro['fl_bf_shift_p5'] = (df_futuro['fl_bf_shift_p5'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)

            df_futuro['fl_bf_shift_p6'] = df_futuro['fl_feriado_bf'].shift(+6)
            df_futuro['fl_bf_shift_p6'] = (df_futuro['fl_bf_shift_p6'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)

            df_futuro['fl_bf_shift_p7'] = df_futuro['fl_feriado_bf'].shift(+7)
            df_futuro['fl_bf_shift_p7'] = (df_futuro['fl_bf_shift_p7'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)

            df_futuro['fl_cm_shift_1'] = df_futuro['fl_feriado_cm'].shift(-1)
            df_futuro['fl_cm_shift_1'] = (df_futuro['fl_cm_shift_1'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)

            df_futuro['fl_cm_shift_2'] = df_futuro['fl_feriado_cm'].shift(-2)
            df_futuro['fl_cm_shift_2'] = (df_futuro['fl_cm_shift_2'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)

            df_futuro['fl_cm_shift_3'] = df_futuro['fl_feriado_cm'].shift(-3)
            df_futuro['fl_cm_shift_3'] = (df_futuro['fl_cm_shift_3'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)

            df_futuro['fl_nt_shift_1'] = df_futuro['fl_feriado_nt'].shift(-1)
            df_futuro['fl_nt_shift_1'] = (df_futuro['fl_nt_shift_1'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)

            df_futuro['fl_nt_shift_2'] = df_futuro['fl_feriado_nt'].shift(-2)
            df_futuro['fl_nt_shift_2'] = (df_futuro['fl_nt_shift_2'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)

            df_futuro['fl_nt_shift_3'] = df_futuro['fl_feriado_nt'].shift(-3)
            df_futuro['fl_nt_shift_3'] = (df_futuro['fl_nt_shift_3'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)

            df_futuro['fl_nt_shift_4'] = df_futuro['fl_feriado_nt'].shift(-4)
            df_futuro['fl_nt_shift_4'] = (df_futuro['fl_nt_shift_4'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)

            df_futuro['fl_nt_shift_5'] = df_futuro['fl_feriado_nt'].shift(-5)
            df_futuro['fl_nt_shift_5'] = (df_futuro['fl_nt_shift_5'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)
                                                
            df_futuro['fl_nt_shift_6'] = df_futuro['fl_feriado_nt'].shift(-6)
            df_futuro['fl_nt_shift_6'] = (df_futuro['fl_nt_shift_6'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)

            df_futuro['fl_nt_shift_7'] = df_futuro['fl_feriado_nt'].shift(-7)
            df_futuro['fl_nt_shift_7'] = (df_futuro['fl_nt_shift_7'].apply(lambda x: 0 if np.isnan(x) else x)).astype(int)

            # 7. Cria variavel do dia da semana, dia, mes, ano, semana do mes
            df_futuro["id_dds"] = df_futuro["dt_pedido"].apply(lambda x:int(x.weekday()) + 2 if int(x.weekday()) + 2 != 8 else 1)
            df_futuro["id_dia"] = df_futuro["dt_pedido"].apply(lambda x: x.day)
            df_futuro["id_semana_do_mes"] = df_futuro["dt_pedido"].apply(lambda x: math.ceil(x.day/7))
            df_futuro["id_mes"] = df_futuro["dt_pedido"].apply(lambda x: x.month)
            df_futuro["id_ano"] = df_futuro["dt_pedido"].apply(lambda x: x.year)
            df_futuro["id_semana_do_ano"] = df_futuro["dt_pedido"].dt.isocalendar().week

            # 8. Cria variaveis dummys para cada dia da semana
            df_futuro["fl_domingo"] = 0
            df_futuro["fl_segunda"] = 0    
            df_futuro["fl_terca"] = 0
            df_futuro["fl_quarta"] = 0
            df_futuro["fl_quinta"] = 0
            df_futuro["fl_sexta"] = 0
            df_futuro["fl_sabado"] = 0
            df_futuro.loc[(df_futuro["id_dds"] == 1,"fl_domingo")] = 1
            df_futuro.loc[(df_futuro["id_dds"] == 2,"fl_segunda")] = 1
            df_futuro.loc[(df_futuro["id_dds"] == 3,"fl_terca")] = 1
            df_futuro.loc[(df_futuro["id_dds"] == 4,"fl_quarta")] = 1
            df_futuro.loc[(df_futuro["id_dds"] == 5,"fl_quinta")] = 1
            df_futuro.loc[(df_futuro["id_dds"] == 6,"fl_sexta")] = 1
            df_futuro.loc[(df_futuro["id_dds"] == 7,"fl_sabado")] = 1

            # 9. Cria variaveis dummys para cada semana dentro do mesmo mes
            df_futuro["fl_sem2_mes"] = 0
            df_futuro["fl_sem3_mes"] = 0    
            df_futuro["fl_sem4_mes"] = 0
            df_futuro["fl_sem5_mes"] = 0
            df_futuro.loc[(df_futuro["id_semana_do_mes"] == 2,"fl_sem2_mes")] = 1
            df_futuro.loc[(df_futuro["id_semana_do_mes"] == 3,"fl_sem3_mes")] = 1
            df_futuro.loc[(df_futuro["id_semana_do_mes"] == 4,"fl_sem4_mes")] = 1
            df_futuro.loc[(df_futuro["id_semana_do_mes"] == 5,"fl_sem5_mes")] = 1

            # 10. Cria variaveis dummys para cada mes
            df_futuro["fl_jan"] = 0
            df_futuro["fl_fev"] = 0
            df_futuro["fl_mar"] = 0
            df_futuro["fl_abr"] = 0
            df_futuro["fl_mai"] = 0
            df_futuro["fl_jun"] = 0
            df_futuro["fl_jul"] = 0
            df_futuro["fl_ago"] = 0
            df_futuro["fl_set"] = 0
            df_futuro["fl_out"] = 0
            df_futuro["fl_nov"] = 0
            df_futuro["fl_dez"] = 0
            df_futuro.loc[(df_futuro["id_mes"] == 1,"fl_jan")] = 1
            df_futuro.loc[(df_futuro["id_mes"] == 2,"fl_fev")] = 1
            df_futuro.loc[(df_futuro["id_mes"] == 3,"fl_mar")] = 1
            df_futuro.loc[(df_futuro["id_mes"] == 4,"fl_abr")] = 1
            df_futuro.loc[(df_futuro["id_mes"] == 5,"fl_mai")] = 1
            df_futuro.loc[(df_futuro["id_mes"] == 6,"fl_jun")] = 1
            df_futuro.loc[(df_futuro["id_mes"] == 7,"fl_jul")] = 1
            df_futuro.loc[(df_futuro["id_mes"] == 8,"fl_ago")] = 1
            df_futuro.loc[(df_futuro["id_mes"] == 9,"fl_set")] = 1
            df_futuro.loc[(df_futuro["id_mes"] == 10,"fl_out")] = 1
            df_futuro.loc[(df_futuro["id_mes"] == 11,"fl_nov")] = 1
            df_futuro.loc[(df_futuro["id_mes"] == 12,"fl_dez")] = 1

            df_futuro = df_futuro.drop(columns=['id_dds','id_dia','id_mes','id_ano','fl_domingo','fl_jan','id_semana_do_ano'])

            # Adicionar todas as colunas usadas no treinamento com valores padrão (zero)
            colunas_treinamento = X_train.columns

            # Garantir que as colunas estejam na mesma ordem
            df_futuro.set_index('dt_pedido', inplace=True)
            df_futuro = df_futuro[colunas_treinamento]

            # df_futuro.to_csv("base_futura.csv", index=True)

            # Fazer previsões para as próximas datas com o melhor modelo
            previsoes = melhor_modelo.predict(df_futuro)

            # Criar um DataFrame com as datas e previsões
            df_resultado = pd.DataFrame({
                'creationdateforecast': futuras_datas,
                'predicted_revenue': previsoes
            })
            df_resultado['predicted_revenue'] = df_resultado['predicted_revenue'].apply(lambda x: round(x, 2)) 
            
            # Salvar os resultados em um CSV
            # df_resultado.to_csv("forecast_a_partir_de_1_nov_24.csv", index=False)
            # print("Projeção salva no arquivo 'forecast_a_partir_de_1_nov_24.csv'.")

            return df_resultado
        else:
            print("QUAL MODELO: MEDIA")
            # hoje = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            hoje=date_start_info
            seis_meses_atras = hoje - timedelta(days=6 * 30)  # Aproximadamente 6 meses
            df_ultimos_seis_meses = df[df['dt_pedido'] >= seis_meses_atras]

            # Adicionar coluna para o dia da semana (0 = segunda-feira, 6 = domingo)
            df_ultimos_seis_meses['dia_semana'] = df_ultimos_seis_meses['dt_pedido'].dt.dayofweek

            # Calcular a média do valor por dia da semana
            media_por_dia_semana = (
                df_ultimos_seis_meses.groupby('dia_semana')['sum_revenue'].mean().reset_index()
            )
            media_por_dia_semana.columns = ['dia_semana', 'predicted_revenue']

            # Criar base futura com intervalo de datas
            # Intervalo de datas futuras (exemplo: próximos 30 dias)
            datas_futuras = pd.date_range(start=hoje + timedelta(days=1), periods=30)

            # Criar DataFrame de datas futuras
            df_futuro = pd.DataFrame({'creationdateforecast': datas_futuras})

            # Adicionar dia da semana para as datas futuras
            df_futuro['dia_semana'] = df_futuro['creationdateforecast'].dt.dayofweek

            # Juntar as médias por dia da semana
            df_futuro = df_futuro.merge(media_por_dia_semana, on='dia_semana', how='left')

            
            if not pd.api.types.is_numeric_dtype(df_futuro['predicted_revenue']):
                df_futuro['predicted_revenue'] = pd.to_numeric(df_futuro['predicted_revenue'], errors='coerce')

            
            return df_futuro[['creationdateforecast', 'predicted_revenue']]

    
    except Exception as e: 
        logging.error(f"An unexpected error occurred while processing the page: {e}")
        raise  # Ensure any error fails the Airflow task


def inserir_forecast(future_df: pd.DataFrame):
    """Insere ou atualiza previsões na tabela, preservando passado."""
    if future_df.empty:
        logging.warning("DataFrame de futuro vazio – nada a inserir.")
        return

    final_df = future_df[["creationdateforecast","predicted_revenue"]].copy()
    final_df["predicted_revenue"] = final_df["predicted_revenue"].round(2)

    #hoje_dt = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    hoje_dt= date_start_info
    hoje_str = hoje_dt.strftime("%Y-%m-%d")

    create_sql = """CREATE TABLE IF NOT EXISTS orders_ia_forecast (
        creationdateforecast TIMESTAMP PRIMARY KEY,
        predicted_revenue NUMERIC NOT NULL
    );"""
    WriteJsonToPostgres(data_conection_info, create_sql).execute_query_ddl()

    delete_sql = f"DELETE FROM orders_ia_forecast WHERE creationdateforecast >= '{hoje_str}';"
    WriteJsonToPostgres(data_conection_info, delete_sql).execute_query_ddl()

    count_sql = "SELECT COUNT(1) AS qtd FROM orders_ia_forecast;"
    _, res = WriteJsonToPostgres(data_conection_info, count_sql).query()
    primeira_execucao = res[0]["qtd"] == 0

    if primeira_execucao:
        print("primeira execucao")
        df_realizado = CriaDataFrameRealizado()

        # -- garanta que dt_pedido é datetime, se ainda não for
        df_realizado["dt_pedido"] = pd.to_datetime(df_realizado["dt_pedido"])

        hoje_dt = pd.to_datetime(hoje_dt)          # caso hoje_dt ainda seja string ou date
        primeiro_dia_mes = hoje_dt.replace(day=1)  # 1º dia do mês de hoje

        # pedidos do mês corrente, até ontem (hoje_dt não incluso)
        filtro = (df_realizado["dt_pedido"] < hoje_dt) & \
                (df_realizado["dt_pedido"] >= primeiro_dia_mes)

        df_hist = df_realizado.loc[filtro, ["dt_pedido", "sum_revenue"]].copy()
        df_hist.columns = ["creationdateforecast", "predicted_revenue"]
        df_hist["predicted_revenue"] = df_hist["predicted_revenue"].astype(float).round(2)
        WriteJsonToPostgres(data_conection_info, df_hist.to_dict("records"), "orders_ia_forecast", "creationdateforecast").insert_data_batch(df_hist.to_dict("records"))

    WriteJsonToPostgres(data_conection_info, final_df.to_dict("records"), "orders_ia_forecast", "creationdateforecast").insert_data_batch(final_df.to_dict("records"))


def set_globals(api_info, data_conection, coorp_conection,date_start, **kwargs):
    global api_conection_info, data_conection_info, coorp_conection_info,date_start_info
    api_conection_info = api_info
    data_conection_info = data_conection
    coorp_conection_info = coorp_conection
    date_start_info= date_start
    
   
    if not all([api_conection_info, data_conection_info, coorp_conection_info]):
        logging.error("Global connection information is incomplete.")
        raise ValueError("All global connection information must be provided.")
    try:
        # data_hoje = datetime.now().strftime('%Y-%m-%d')
        
        projecao = gerar_projecao_a_partir_de_data(date_start_info)

        inserir_forecast(projecao)

    except Exception as e: 
        logging.error(f"An unexpected error occurred while processing the page: {e}")
        raise  # Ensure any error fails the Airflow task



