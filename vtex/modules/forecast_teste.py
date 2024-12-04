
import pandas as pd
from teste_dbpgconn import WriteJsonToPostgres
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime, timedelta
import math



from sklearn.model_selection import train_test_split
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.inspection import permutation_importance
from xgboost import XGBRegressor
from lightgbm import LGBMRegressor
from catboost import CatBoostRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_log_error
import numpy as np
from prophet import Prophet




#nção para calcular o RMSLE
def rmsle(y_true, y_pred):
    return np.sqrt(mean_squared_log_error(y_true + 1, y_pred + 1))  # Adiciona 1 para evitar log(0)


def CriaDataFrameFeriado(schema= "6cc09227-eb0f-482f-82fa-a827fd538072"):
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
        _, feriado = WriteJsonToPostgres("integrations-data-dev", query_feriado, "tb_forecast_feriado").query()

        df_feriado = pd.DataFrame(feriado)
        df_feriado = df_feriado.rename(columns={'dt_feriado': 'dt_pedido'})
        df_feriado['dt_pedido'] = df_feriado['dt_pedido'].apply(lambda x: x.strftime('%Y-%m-%d'))
        df_feriado['dt_pedido'] = pd.to_datetime(df_feriado['dt_pedido'])
        # Return
        return df_feriado
    except Exception as e: 
        print(e)
        return 0

 
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
        _, result = WriteJsonToPostgres("integrations-data-dev", query_realizado, "orders_ia").query()
        df_new = pd.DataFrame(result)
        # Return
        return df_new
    except Exception as e: 
        print(e)
        return 0


def tratar_base(schema,horizonte_forecast):
   

    # Cria dataframe com os dados historicos do faturamento realizado
    df_realizado = CriaDataFrameRealizado(schema)

    # Cria dataframe com as referencias de feriados passados e futuros (ate 2030)
    df_feriado = CriaDataFrameFeriado()
    
    df_realizado['sum_revenue'] = pd.to_numeric(df_realizado['sum_revenue'], errors='coerce')

    # Cria dataframe com o horizonte futuro de forecast
    #min_realizado = str(df_realizado['dt_pedido'].min())[:10]
    min_realizado = str(df_realizado['dt_pedido'].min())[:10]
    max_realizado = str(df_realizado['dt_pedido'].max())[:10]
    print(min_realizado)
    print(max_realizado)

    d1 = datetime.strptime(min_realizado,"%Y-%m-%d")
    d2 = datetime.strptime(max_realizado,"%Y-%m-%d")
    dif_days = abs((d2-d1).days) +1

    df_horizonte = pd.DataFrame({'dt_pedido': pd.date_range(min_realizado, periods = dif_days + horizonte_forecast)})
    
    # Merge dos dataframes e separacao dos feriados de black friday e cyber monday
    df_full = df_horizonte.merge(df_realizado, on='dt_pedido', how='left')
    df_full = df_full.merge(df_feriado, on='dt_pedido', how='left')
  
    
    df_full['fl_feriado_ativo'] = df_full['fl_feriado_ativo'].apply(lambda x: 0 if (np.isnan(x) or x == 0) else 1)
    df_full['fl_feriado_bf'] = df_full['fl_feriado_bf'].apply(lambda x: 0 if (np.isnan(x) or x == 0) else 1)
    df_full['fl_feriado_cm'] = df_full['fl_feriado_cm'].apply(lambda x: 0 if (np.isnan(x) or x == 0) else 1)
    df_full['nm_tipo_registro'] = df_full['nm_tipo_registro'].fillna('forecast')
   
    
    #df_full.tail(20)
  
    # Cria as defasagens dos feriados normais
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
  
   # Cria as defasagens dos feriados black friday e cyber monday
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
    
    df_full['fl_blackfriday'] = df_full['dt_pedido'].apply(lambda x: 1 if ((x.month == 11 and x.day >= 25) or (x.month == 12 and x.day <= 1)) else 0)


    # Cria variavel do dia da semana, dia, mes, ano, semana do mes
    df_full["id_dds"] = df_full["dt_pedido"].apply(lambda x:int(x.weekday()) + 2 if int(x.weekday()) + 2 != 8 else 1)
    df_full["id_dia"] = df_full["dt_pedido"].apply(lambda x: x.day)
    df_full["id_semana_do_mes"] = df_full["dt_pedido"].apply(lambda x: math.ceil(x.day/7))
    df_full["id_mes"] = df_full["dt_pedido"].apply(lambda x: x.month)
    df_full["id_ano"] = df_full["dt_pedido"].apply(lambda x: x.year)
    df_full["id_semana_do_ano"] = df_full["dt_pedido"].dt.isocalendar().week


    # # Cria variaveis dummys para cada dia da semana
    # df_full["fl_domingo"] = 0
    # df_full["fl_segunda"] = 0    
    # df_full["fl_terca"] = 0
    # df_full["fl_quarta"] = 0
    # df_full["fl_quinta"] = 0
    # df_full["fl_sexta"] = 0
    # df_full["fl_sabado"] = 0
    # df_full.loc[(df_full["id_dds"] == 1,"fl_domingo")] = 1
    # df_full.loc[(df_full["id_dds"] == 2,"fl_segunda")] = 1
    # df_full.loc[(df_full["id_dds"] == 3,"fl_terca")] = 1
    # df_full.loc[(df_full["id_dds"] == 4,"fl_quarta")] = 1
    # df_full.loc[(df_full["id_dds"] == 5,"fl_quinta")] = 1
    # df_full.loc[(df_full["id_dds"] == 6,"fl_sexta")] = 1
    # df_full.loc[(df_full["id_dds"] == 7,"fl_sabado")] = 1
 
    # # Cria variaveis dummys para cada mes
    # df_full["fl_jan"] = 0
    # df_full["fl_fev"] = 0
    # df_full["fl_mar"] = 0
    # df_full["fl_abr"] = 0
    # df_full["fl_mai"] = 0
    # df_full["fl_jun"] = 0
    # df_full["fl_jul"] = 0
    # df_full["fl_ago"] = 0
    # df_full["fl_set"] = 0
    # df_full["fl_out"] = 0
    # df_full["fl_nov"] = 0
    # df_full["fl_dez"] = 0
    # df_full.loc[(df_full["id_mes"] == 1,"fl_jan")] = 1
    # df_full.loc[(df_full["id_mes"] == 2,"fl_fev")] = 1
    # df_full.loc[(df_full["id_mes"] == 3,"fl_mar")] = 1
    # df_full.loc[(df_full["id_mes"] == 4,"fl_abr")] = 1
    # df_full.loc[(df_full["id_mes"] == 5,"fl_mai")] = 1
    # df_full.loc[(df_full["id_mes"] == 6,"fl_jun")] = 1
    # df_full.loc[(df_full["id_mes"] == 7,"fl_jul")] = 1
    # df_full.loc[(df_full["id_mes"] == 8,"fl_ago")] = 1
    # df_full.loc[(df_full["id_mes"] == 9,"fl_set")] = 1
    # df_full.loc[(df_full["id_mes"] == 10,"fl_out")] = 1
    # df_full.loc[(df_full["id_mes"] == 11,"fl_nov")] = 1
    # df_full.loc[(df_full["id_mes"] == 12,"fl_dez")] = 1
    
    

    df_full_tratado = df_full.drop(columns=['nm_tipo_registro'])


    # for lag in range(1, 120):  # Lags de 1 até 7 dias
    #     df_full_tratado[f'sum_revenue_lag{lag}'] = df_full_tratado['sum_revenue'].shift(lag)
    # colunas_lag = [f'sum_revenue_lag{lag}' for lag in range(1, 120)]
    # for coluna in colunas_lag:
    #     df_full_tratado[coluna] = pd.to_numeric(df_full_tratado[coluna], errors='coerce')
    
    # #df_full_tratado.to_csv(f"Base_tratada_{schema}.csv", index=False)
    # df_full_tratado = df_full_tratado[~((df_full_tratado['nm_tipo_registro'] == "realizado") & df_full_tratado.isna().any(axis=1))]
    
    # for lag in range(375, 120, -1):
    #     df_full_tratado[f'sum_revenue_lag{lag}'] = df_full_tratado['sum_revenue'].shift(lag)


    df_full_tratado['sum_revenue_365d'] = df_full_tratado['sum_revenue'].shift(365)
    df_full_tratado['media_90d_ano_anterior'] = df_full_tratado['sum_revenue'].shift(365).rolling(window=90).mean()
    df_full_tratado['media_30d_ano_anterior'] = df_full_tratado['sum_revenue'].shift(365).rolling(window=30).mean()
    df_full_tratado['media_60d_ano_anterior'] = df_full_tratado['sum_revenue'].shift(365).rolling(window=60).mean()
    
    df_full_tratado['sum_revenue_180d'] = df_full_tratado['sum_revenue'].shift(180)
    df_full_tratado['sum_revenue_121d'] = df_full_tratado['sum_revenue'].shift(121)
    
    df_full_tratado['media_90d_ano_atual'] = df_full_tratado['sum_revenue'].shift(121).rolling(window=90).mean()
    df_full_tratado['media_30d_ano_atual'] = df_full_tratado['sum_revenue'].shift(121).rolling(window=30).mean()
    df_full_tratado['media_60d_ano_atual'] = df_full_tratado['sum_revenue'].shift(121).rolling(window=60).mean()

    
    df_full_tratado.to_csv(f"Base_tratada_{schema}.csv", index=False)
    df_full_tratado.dropna(inplace=True)
    
  #  df_full_tratado.info()
    
    return df_full_tratado




def varivel_importante(schema, dias_projecao):
    df = tratar_base(schema, dias_projecao)
    df.set_index('dt_pedido', inplace=True)

    X = df.drop(columns=['sum_revenue'])
    y = df['sum_revenue']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)

    models = {
        'LGBMRegressor': LGBMRegressor(),
        'HistGradientBoostingRegressor': HistGradientBoostingRegressor(),
        'XGBRegressor': XGBRegressor(),
        'CatBoostRegressor': CatBoostRegressor(verbose=0),
        'RandomForest': RandomForestRegressor(random_state=42, n_estimators=100)
    }

    # Dicionário para armazenar as importâncias
    importances = {}

    for name, model in models.items():
        model.fit(X_train, y_train)  # Treinar o modelo

        if name == 'CatBoostRegressor':
            # Para CatBoost
            feature_importance = model.get_feature_importance(prettified=False)
        elif name == 'HistGradientBoostingRegressor':
            # Para HistGradientBoosting usar permutation_importance
            perm_importance = permutation_importance(model, X_train, y_train, n_repeats=10, random_state=42)
            feature_importance = perm_importance.importances_mean
        else:
            # Para os outros modelos
            feature_importance = model.feature_importances_

        importances[name] = feature_importance

    # Consolidar as importâncias em um DataFrame
    features = X.columns
    importances_df = pd.DataFrame(importances, index=features)
    importances_df['Average Importance'] = importances_df.mean(axis=1)
    importances_df = importances_df.sort_values(by='Average Importance', ascending=False)

    # Exibir o DataFrame com as importâncias das variáveis
    importances_df.reset_index(inplace=True)
    importances_df.rename(columns={'index': 'Feature'}, inplace=True)

    # Retornar o DataFrame para análise
    return importances_df



def forecast_prod(schema,dias_projecao):

    
    df=tratar_base(schema,dias_projecao)
   
    df.set_index('dt_pedido', inplace=True)
    

        
    X = df.drop(columns=['sum_revenue'])
    y = df['sum_revenue']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)

    modelos = {
        'LGBMRegressor': LGBMRegressor(),
        'HistGradientBoostingRegressor': HistGradientBoostingRegressor(),
        'XGBRegressor': XGBRegressor(),
        'CatBoostRegressor': CatBoostRegressor(verbose=0),
        'RandomForest': RandomForestRegressor(random_state=42, n_estimators=100)
    }

    resultados = []
    

    resultados = []
    previsoes_modelos = {}

    for nome, modelo in modelos.items():
        modelo.fit(X_train, y_train)
        previsoes = modelo.predict(X_test)
        erro_mae = mean_absolute_error(y_test, previsoes)
        erro_rmsle = rmsle(y_test, previsoes)
        resultados.append({'modelo': nome, 'mean_absolute_error': erro_mae, 'rmsle': erro_rmsle})
        previsoes_modelos[nome] = modelo

    # Seleção do melhor modelo com base no menor RMSLE
    resultados_df = pd.DataFrame(resultados)
    melhor_modelo_nome = resultados_df.loc[resultados_df['rmsle'].idxmin(), 'modelo']
    melhor_modelo = previsoes_modelos[melhor_modelo_nome]

    print(f"\nMelhor modelo: {melhor_modelo_nome}")

    # Preparação para projeção
    ultima_data = df.index.max()
    datas_futuras = pd.date_range(ultima_data + timedelta(days=1), 
                                  periods=dias_projecao, 
                                  freq='D')
    datas_anteriores = pd.date_range(ultima_data - timedelta(days=30), 
                                     periods=30, 
                                     freq='D')

    # Criação do DataFrame para projeções
    df_futuro = pd.DataFrame(index=datas_futuras, columns=X.columns)
    df_anterior = pd.DataFrame(index=datas_anteriores, columns=X.columns)

    df_futuro['previsao'] = melhor_modelo.predict(df_futuro)
    df_anterior['previsao'] = melhor_modelo.predict(df_anterior)

    # Concatenando previsões
    df_forecast = pd.concat([df_anterior, df, df_futuro])

    #df_forecast.to_csv(f"Base_tratada_{schema}.csv", index=False)
    
    print("Projeção realizada com sucesso!")
    return df_forecast



def teste_forecast(schema,dias_projecao):

    
    df=tratar_base(schema,dias_projecao)
   
    df.set_index('dt_pedido', inplace=True)


    df_check_variavel=varivel_importante("07e14abb-bf37-4022-b159-03d2a757b40b",90)
    print(df_check_variavel)
    # limiar_importancia = 0
    # features_para_remover = df_check_variavel[df_check_variavel['Average Importance'] < limiar_importancia]['Feature'].tolist()

    # # 2. Remover as colunas do DataFrame
    # df = df.drop(columns=features_para_remover)
        

        
    X = df.drop(columns=['sum_revenue'])
    y = df['sum_revenue']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)

    modelos = {
        'LGBMRegressor': LGBMRegressor(),
        'HistGradientBoostingRegressor': HistGradientBoostingRegressor(),
        'XGBRegressor': XGBRegressor(),
        'CatBoostRegressor': CatBoostRegressor(verbose=0),
        'RandomForest': RandomForestRegressor(random_state=42, n_estimators=100)
    }

    resultados = []
    for nome, modelo in modelos.items():
        modelo.fit(X_train, y_train)
        previsoes = modelo.predict(X_test)
        erro_mae = mean_absolute_error(y_test, previsoes)
        erro_rmsle = rmsle(y_test, previsoes)
        resultados.append({'modelo': nome, 'mean_absolute_error': erro_mae, 'rmsle': erro_rmsle})
        print(f'{nome} - MAE: {erro_mae:.2f}, RMSLE: {erro_rmsle:.2f}')

        # plt.figure(figsize=(10, 5))
        # plt.plot(y_test.index, y_test, label='Valor Real')
        # plt.plot(y_test.index, previsoes, label='Previsão')
        # plt.title(f'Previsão de Faturamento - {nome}')
        # plt.xlabel('Data')
        # plt.ylabel('Faturamento')
        # plt.legend()
        # plt.show()

    df['media_movel_7d'] = df['sum_revenue'].rolling(window=7).mean()
    df.dropna(inplace=True)
    X = df.drop(columns=['sum_revenue'])
    y = df['sum_revenue']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)
    previsoes_baseline = X_test['media_movel_7d']
    erro_mae_baseline = mean_absolute_error(y_test, previsoes_baseline)
    erro_rmsle_baseline = rmsle(y_test, previsoes_baseline)
    resultados.append({'modelo': 'Baseline', 'mean_absolute_error': erro_mae_baseline, 'rmsle': erro_rmsle_baseline})
    print(f'Baseline - MAE: {erro_mae_baseline:.2f}, RMSLE: {erro_rmsle_baseline:.2f}')

    try:
        df_prophet = df.reset_index()[['dt_pedido', 'sum_revenue']].rename(columns={'dt_pedido': 'ds', 'sum_revenue': 'y'})
        prophet_model = Prophet()
        prophet_model.fit(df_prophet[:-len(y_test)])
        future = prophet_model.make_future_dataframe(periods=len(y_test))
        previsoes_prophet = prophet_model.predict(future)['yhat'][-len(y_test):]
        erro_mae_prophet = mean_absolute_error(y_test, previsoes_prophet)
        erro_rmsle_prophet = rmsle(y_test, previsoes_prophet)
        resultados.append({'modelo': 'Prophet', 'mean_absolute_error': erro_mae_prophet, 'rmsle': erro_rmsle_prophet})
        print(f'Prophet - MAE: {erro_mae_prophet:.2f}, RMSLE: {erro_rmsle_prophet:.2f}')
    except Exception as e:
        print(f"Erro ao ajustar o modelo Prophet: {e}")


    metrics = pd.DataFrame(resultados).round(2).sort_values(by="mean_absolute_error")
    print("\nResultados Comparativos:")
    print(metrics)




teste_forecast("07e14abb-bf37-4022-b159-03d2a757b40b",120)


