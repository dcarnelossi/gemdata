

from modules.dbpgconn import *

from modules.save_to_blob import *
import subprocess
import sys
import os
import datetime
from dateutil.relativedelta import relativedelta

import uuid
import shutil

# Função para instalar um pacote via pip
def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

# Instalar matplotlib se não estiver instalado
try:
    import matplotlib.pyplot as plt
    import matplotlib.patches as patches
except ImportError:
    print("matplotlib não está instalado. Instalando agora...")
    install("matplotlib")
    import matplotlib.pyplot as plt
    import matplotlib.patches as patches


# Instalar matplotlib se não estiver instalado
try:
    from fpdf import FPDF
except ImportError:
    print("matplotlib não está instalado. Instalando agora...")
    install("fpdf2")
    from fpdf import FPDF


# Instalar matplotlib se não estiver instalado
try:
    import numpy as np
except ImportError:
    print("matplotlib não está instalado. Instalando agora...")
    install("numpy")
    import numpy as np






data_conection_info = None
idintegration = None
celular = None
logo = None
caminho_pdf_blob = None



def formatar_numerocasas_br(valor,casas,ismoeda):
    valor_arred = round(valor,casas)
    if(ismoeda == 1):
        valor_str = f"R$ {valor_arred:,.{casas}f}"
    else:    
        valor_str = f"{valor_arred:,.{casas}f}"

    valor_str = valor_str.replace('.', 'x')
    valor_str = valor_str.replace(',', '.')
    valor_str = valor_str.replace('x', ',')
    return valor_str


def abreviar_valor_br(valor,ismoeda,tipoabreviacao):
        if tipoabreviacao == 3:
            return f'{formatar_numerocasas_br(valor/1000000000,1,ismoeda)}B'
        elif tipoabreviacao == 2:
            return f'{formatar_numerocasas_br(valor/1000000,1,ismoeda)}M'
        elif tipoabreviacao == 1:
            return f'{formatar_numerocasas_br(valor/1000,1,ismoeda)}K'
        return f'{formatar_numerocasas_br(valor,0,ismoeda)}'


def abreviar_eixoy_moeda(valor,casas,ismoeda,tipoabreviacao):
        if tipoabreviacao == 3:
            return f'{formatar_numerocasas_br(valor/1000000000,casas,ismoeda)}B'
        elif tipoabreviacao == 2:
            return f'{formatar_numerocasas_br(valor/1000000,casas,ismoeda)}M'
        elif tipoabreviacao == 1:
            return f'{formatar_numerocasas_br(valor/1000,casas,ismoeda)}K'
        return f'{formatar_numerocasas_br(valor,casas,ismoeda)}'


# Função para atribuir valores com base nos intervalos
def classify_ranking(ranking):
    if 1 <= ranking <= 10:
        return 1
    elif 10 <= ranking <= 20:
        return 2
    elif 20 <= ranking <= 30:
        return 3
    elif 30 <= ranking <= 40:
       return 3
    else:
        return 4  # Caso deseje lidar com valores fora dos intervalos

def getbase(celular,integration,diretorio):
    try:
        query =f""" 
					select 
                    cast(idprod as int) as idprod,
                    namesku,
                    cast(sum(revenue_without_shipping) as float) as  revenue_without_shipping,
                    cast(count(1) as int) as pedidos  ,
                    cast(sum(revenue_orders_out_ship) as float) as revenue_orders,
                    cast(sum(revenue_orders_out_ship)/ count(distinct orderid) as float) as tickemedio,
                    cast(sum(revenue_orders_out_ship) - sum(revenue_without_shipping) as float) as receita_incremental

                    from "{integration}".orders_items_ia
                    where
                    date_trunc('month',COALESCE(creationdate, '1900-01-01'))::date >= date_trunc('month', CURRENT_DATE) - INTERVAL '7 month'
                    and 
                    date_trunc('month',COALESCE(creationdate, '1900-01-01'))::date <= date_trunc('month', CURRENT_DATE) - INTERVAL '1 month'
                   
                    group by 1,2 


                    --limit 3

                    """
       
        
        # _, result = WriteJsonToPostgres("integrations-data-dev", query, "orders_items_ia").query()
        df_ticket=WriteJsonToPostgres(data_conection_info,query).query_dataframe()

        if len(df_ticket)==0:
            logging.warning("No skus found in the database.")
            return False
        

        query_pedidos =f""" 
					select 
                    cast(count(distinct orderid) as int) as pedidos
                    from "{integration}".orders_items_ia
                    where
                    date_trunc('month',COALESCE(creationdate, '1900-01-01'))::date >= date_trunc('month', CURRENT_DATE) - INTERVAL '6 month'
                    and 
                    date_trunc('month',COALESCE(creationdate, '1900-01-01'))::date <= date_trunc('month', CURRENT_DATE) - INTERVAL '1 month'
                   
                    """
       
        
        # _, result_pedidos = WriteJsonToPostgres("integrations-data-dev", query_pedidos, "orders_items_ia").query()

        df_pedidos=WriteJsonToPostgres(data_conection_info,query_pedidos).query_dataframe()
        if len(df_pedidos) ==0 :
            logging.warning("No skus found in the database.")
            return False
        # df_pedidos = pd.DataFrame(result_pedidos)    
        #print(result) 
        #df_tupla = pd.DataFrame(result, columns=['idprod', 'namesku', 'revenue_without_shipping', 'pedidos', 'revenue_orders', 'tickemedio', 'receita_incremental'])
       # df_ticket = pd.DataFrame(result)
        qtd_prod_total = df_ticket['idprod'].count()
        
        #ordenando do dataframe 
        df_ticket= df_ticket.sort_values(by='revenue_without_shipping', ascending=False).reset_index(drop=True)
        #Fazendo o percentual de cada sku sobre o total 
        df_ticket['percentualtotal'] = df_ticket['revenue_without_shipping'] / df_ticket['revenue_without_shipping'].sum()
        #acumulando o percentual de cada sku  
        df_ticket['percacumuladototal'] = (df_ticket['percentualtotal'].cumsum())*100
        
        #fazendo o ticket medio total da curva a b c  
        ticket_medio_geral = df_ticket['revenue_without_shipping'].sum()/df_pedidos['pedidos'].sum()

        #filtrando e jogando em outro dataframe tudo que for < 80% do percentual acumulado (curva A)
        df_ticket_curvaa = df_ticket.loc[df_ticket['percacumuladototal'] <80]
        df_ticket_curvaa = df_ticket_curvaa.copy()
        #vendo quantos produtos ficaram na curva A e fazendo o numero participação geral
        qtd_prod = df_ticket_curvaa['idprod'].count()
        qtd_part = 1/qtd_prod
 
        #fazendo o numero de participação do faturamento apenas com a curva A 
        df_ticket_curvaa['participacaocurvaa'] = df_ticket_curvaa['revenue_without_shipping'] / df_ticket_curvaa['revenue_without_shipping'].sum()
        #Fazendo o Score participacao
        df_ticket_curvaa['score_part_curvaa'] = ((df_ticket_curvaa['participacaocurvaa']-qtd_part)*100 )*3

        #fazendo o score do ticket   da curva a  
        df_ticket_curvaa['var_vs_med_curvaa'] = (df_ticket_curvaa['tickemedio']/ticket_medio_geral)-1
        df_ticket_curvaa['score_ticket_curvaa'] =df_ticket_curvaa['var_vs_med_curvaa']*4
        
        #fazendo o score do incremento da curva a
        df_ticket_curvaa['fator_incr_curvaa'] = df_ticket_curvaa['receita_incremental']/ df_ticket_curvaa['revenue_without_shipping'] 
        df_ticket_curvaa['score_incremental_curvaa'] = (df_ticket_curvaa['receita_incremental']/ df_ticket_curvaa['revenue_without_shipping']) *6

        df_ticket_curvaa['score_ticket_total_curvaa'] =  df_ticket_curvaa['score_part_curvaa'] +  df_ticket_curvaa['score_ticket_curvaa']+ df_ticket_curvaa['score_incremental_curvaa']

        

       #print(df_ticket_curvaa)

        query_item =f""" 
				

            WITH data_base AS (
                        select distinct
                            orderid 
                            ,date_trunc('month', CURRENT_DATE) - INTERVAL '1 month' as datafim 
                            ,date_trunc('month', CURRENT_DATE) - INTERVAL '6 month'as dataini
                        from "{integration}".orders_items_ia b 
                )
				select  
                    cast(idprod as int) as idprod,
                    namesku,
                    CAST(sum(revenue_without_shipping)/6 AS float) AS  revenue_6meses,
                    cast(sum(CASE
		                   WHEN  date_trunc('month',COALESCE(creationdate, '1900-01-01'))::date >= 
		                   			date_trunc('month', d.datafim) - INTERVAL '2 month'
		                   then revenue_without_shipping
		        			else
	        				0 end)/3 as float  ) AS  revenue_3meses,
	                	
	                cast(sum(CASE
		                        WHEN  date_trunc('month',COALESCE(creationdate, '1900-01-01'))::date >= 
		                   			date_trunc('month', datafim)
		                        then revenue_without_shipping
		        			    else
	        				    0 end) as float  ) AS  revenue_1mes,

	                cast(sum(CASE
		                   WHEN  COALESCE(creationdate, '1900-01-01')::date >= (datafim - INTERVAL '13 weeks')
                           and   COALESCE(creationdate, '1900-01-01')::date < (datafim - INTERVAL '1 weeks')
		                   then revenue_without_shipping
		        			else
	        				0 end)/12 as float  ) AS  revenue_12semanas,
	                	          	
	                cast(sum(CASE
		                   WHEN  COALESCE(creationdate, '1900-01-01')::date >= (datafim - INTERVAL '9 weeks')
		                  	and  COALESCE(creationdate, '1900-01-01')::date < (datafim - INTERVAL '1 weeks')
		                   then revenue_without_shipping
		        			else
	        				0 end)/8 as float  ) AS  revenue_8semanas,
	                
                    cast(sum(
                    	CASE
		                   WHEN  COALESCE(creationdate, '1900-01-01')::date >= (datafim - INTERVAL '5 weeks')
		                   and  COALESCE(creationdate, '1900-01-01')::date < (datafim - INTERVAL '1 weeks')
		                   then revenue_without_shipping
		        			else
	        				0
	                	end)/4 as float  ) AS  revenue_4semanas
	                	
                       from "{integration}".orders_items_ia o
                       left join data_base d on
                       d.orderid = o.orderid
                   
                    where
                    date_trunc('month',COALESCE(creationdate, '1900-01-01'))::date >= dataini
                    and 
                    date_trunc('month',COALESCE(creationdate, '1900-01-01'))::date <= datafim 
                  
                    group by 1,2 ;

                    """
       
        
        df_tendencia=WriteJsonToPostgres(data_conection_info,query_item).query_dataframe()
        if len(df_tendencia) ==0 :
            logging.warning("No skus found in the database.")
            return False
        #print(result) 
        #df_tupla = pd.DataFrame(result, columns=['idprod', 'namesku', 'revenue_without_shipping', 'pedidos', 'revenue_orders', 'tickemedio', 'receita_incremental'])
    

    
        # Filtrar o DataFrame completo com base na coluna 'idprod' do DataFrame filtrado
        df_tendencia_curvaa = df_tendencia[df_tendencia['idprod'].isin(df_ticket_curvaa['idprod'])]
        df_tendencia_curvaa = df_tendencia_curvaa.copy()

        
        # Fazendo a variação de 3 meses por 6 meses
        df_tendencia_curvaa['var_3mvs6m'] = np.where(
            df_tendencia_curvaa['revenue_6meses'] == 0,  # Condição para verificar se revenue_6meses é 0
            1.0,  # Colocar 100% (1.0) se revenue_6meses for 0
        (df_tendencia_curvaa['revenue_3meses'] / df_tendencia_curvaa['revenue_6meses']) - 1  # Caso contrário, calcular a variação
        )    
        
        #fazendo a variacao de 1 mes por 6 meses
        df_tendencia_curvaa['var_1mvs6m'] = np.where(
            df_tendencia_curvaa['revenue_6meses'] == 0,  # Condição para verificar se revenue_6meses é 0
            1.0,  # Colocar 100% (1.0) se revenue_6meses for 0
       (df_tendencia_curvaa['revenue_1mes']/df_tendencia_curvaa['revenue_6meses'])-1 # Caso contrário, calcular a variação
        )   
        
        
        #fazendo a variacao de 1 mes por 3 meses
       
        df_tendencia_curvaa['var_1mvs3m'] = np.where(
            df_tendencia_curvaa['revenue_3meses'] == 0,  # Condição para verificar se revenue_6meses é 0
            1.0,  # Colocar 100% (1.0) se revenue_6meses for 0
       (df_tendencia_curvaa['revenue_1mes']/df_tendencia_curvaa['revenue_3meses'])-1 # Caso contrário, calcular a variação
        )  
        

        
        #fazendo a variacao de 12 semanas por 8 semanas
        df_tendencia_curvaa['var_12semvs8sem'] = np.where(
            df_tendencia_curvaa['revenue_8semanas'] == 0,  # Condição para verificar se revenue_6meses é 0
            1.0,  # Colocar 100% (1.0) se revenue_6meses for 0
       (df_tendencia_curvaa['revenue_8semanas']/df_tendencia_curvaa['revenue_12semanas'])-1 # Caso contrário, calcular a variação
        )  
        
        
        #fazendo a variacao de 4 semanas por 12 semanas
        df_tendencia_curvaa['var_4semvs12sem'] = np.where(
            df_tendencia_curvaa['revenue_12semanas'] == 0,  # Condição para verificar se revenue_6meses é 0
            1.0,  # Colocar 100% (1.0) se revenue_6meses for 0
       (df_tendencia_curvaa['revenue_4semanas']/df_tendencia_curvaa['revenue_12semanas'])-1 # Caso contrário, calcular a variação
        )  
        
        #fazendo a variacao de 4 semanas por 8 semanas
        df_tendencia_curvaa['var_4semvs8sem'] = np.where(
            df_tendencia_curvaa['revenue_8semanas'] == 0,  # Condição para verificar se revenue_6meses é 0
            1.0,  # Colocar 100% (1.0) se revenue_6meses for 0
        (df_tendencia_curvaa['revenue_4semanas']/df_tendencia_curvaa['revenue_8semanas'])-1 # Caso contrário, calcular a variação
        )  
        
        
        #fazendo o rating da tendencia
        df_tendencia_curvaa['rating_tendencia'] = (df_tendencia_curvaa['var_3mvs6m'] + 
                                                  df_tendencia_curvaa['var_1mvs6m'] +
                                                    (df_tendencia_curvaa['var_1mvs3m']*2) +
                                                      (df_tendencia_curvaa['var_12semvs8sem']*2) +
                                                          (df_tendencia_curvaa['var_4semvs12sem']*3) + 
                                                          (df_tendencia_curvaa['var_4semvs8sem']*3))/12
        
        #fazendo o score rating da tendencia
        df_tendencia_curvaa['score_rating'] = (1+df_tendencia_curvaa['rating_tendencia'].abs()) ** 2

        #fazendo a uniao do dataframe ticket com tendencia 
        df_merged = pd.merge(df_ticket_curvaa, df_tendencia_curvaa, on='idprod', how='inner')  # Inner join

         #fazendo o score final da tendencia curva a
        df_merged['score_total_tendencia_curvaa']=  df_merged['score_rating']  + df_merged['score_part_curvaa']

         #criando o ranking de ticket medio em cima do score final de ticket medio
        df_merged['ranking_ticket'] = df_merged['score_ticket_total_curvaa'].rank(ascending=False, method='first').astype(int)

         #criando o ranking da tendencia em cima do score final da tendencia
        df_merged['ranking_tendencia'] = df_merged['score_total_tendencia_curvaa'].rank(ascending=False, method='first').astype(int)

        #score final 
        df_merged['score_final'] =  ((df_merged['ranking_ticket']*2 ) + (df_merged['ranking_tendencia']*3) )/5

        #ordenando do menor para maior do score 
        df_merged= df_merged.sort_values(by='score_final', ascending=True).reset_index(drop=True)

        df_merged['classe_score_final'] = df_merged['score_final'].apply(classify_ranking)

        #######################
        #  #classificando os quartis do Faturamento  
        revenue_25=np.percentile( df_merged['revenue_without_shipping'], 25)
        revenue_50=np.percentile( df_merged['revenue_without_shipping'], 50)
        revenue_75=np.percentile( df_merged['revenue_without_shipping'], 75)
        # Definir as condições e os valores correspondentes
        conditions = [
                df_merged['revenue_without_shipping'] < revenue_25,  # Menor que o 1º quartil
                (df_merged['revenue_without_shipping'] >= revenue_25) & (df_merged['revenue_without_shipping'] < revenue_50),  # Entre 25% e 50%
                (df_merged['revenue_without_shipping'] >= revenue_50) & (df_merged['revenue_without_shipping'] < revenue_75),  # Entre 50% e 75%
                df_merged['revenue_without_shipping'] >= revenue_75  # Maior ou igual ao 3º quartil
                    ]
        choices = [0, 1, 2, 3]
        df_merged['revenue_without_shipping_quartis'] =  np.select(conditions, choices)





        #######################
        #  #classificando os quartis de tendencia                    
        tend_menor_25=np.percentile(df_merged.loc[df_merged['rating_tendencia'] <= 0, 'rating_tendencia'], 75)
        tend_menor_50=np.percentile(df_merged.loc[df_merged['rating_tendencia'] <= 0, 'rating_tendencia'], 50)
        #tend_menor_75=np.percentile( df_merged.loc[df_merged['rating_tendencia'] <= 0, 'rating_tendencia'], 75)
        tend_maior_25=np.percentile( df_merged.loc[df_merged['rating_tendencia'] > 0, 'rating_tendencia'], 25)
        tend_maior_50=np.percentile( df_merged.loc[df_merged['rating_tendencia'] > 0, 'rating_tendencia'], 50)
        #tend_maior_75=np.percentile(df_merged.loc[df_merged['rating_tendencia'] > 0, 'rating_tendencia'], 75)
        # Definir as condições e os valores correspondentes
        conditions_tend = [
                df_merged['rating_tendencia'] <= tend_menor_50,  # Menor que o 1º quartil
                (df_merged['rating_tendencia'] > tend_menor_50) & (df_merged['rating_tendencia'] <= tend_menor_25),
                  (df_merged['rating_tendencia'] > tend_menor_25) & (df_merged['rating_tendencia'] <= 0),  # Entre 25% e 50%
                (df_merged['rating_tendencia'] > 0) & (df_merged['rating_tendencia'] <= tend_maior_25), 
                (df_merged['rating_tendencia'] > tend_maior_25) & (df_merged['rating_tendencia'] <= tend_maior_50), # Entre 50% e 75%
                df_merged['rating_tendencia'] > tend_maior_50  # Maior ou igual ao 3º quartil
                    ]
        choices_tend = [-3,-2,-1,1,2,3]
        df_merged['tendencia_quartis'] =  np.select(conditions_tend, choices_tend)


        #######################
        #  #classificando os quartis de ticket medio
        tickemedio_25=np.percentile( df_merged['tickemedio'], 25)
        tickemedio_50=np.percentile( df_merged['tickemedio'], 50)
        tickemedio_75=np.percentile( df_merged['tickemedio'], 75)
        # Definir as condições e os valores correspondentes
        conditions_tm = [
                df_merged['tickemedio'] < tickemedio_25,  # Menor que o 1º quartil
                (df_merged['tickemedio'] >= tickemedio_25) & (df_merged['tickemedio'] < tickemedio_50),  # Entre 25% e 50%
                (df_merged['tickemedio'] >= tickemedio_50) & (df_merged['tickemedio'] < tickemedio_75),  # Entre 50% e 75%
                df_merged['tickemedio'] >= tickemedio_75  # Maior ou igual ao 3º quartil
                    ]
        choices_tm = [1,2, 3, 4]
        df_merged['tickemedio_quartis'] =  np.select(conditions_tm, choices_tm)
       
        #######################
        #  #classificando os quartis de receita incremental
        receitaincremetal_25=np.percentile( df_merged['receita_incremental'], 25)
        receitaincremetal_50=np.percentile( df_merged['receita_incremental'], 50)
        receitaincremetal_75=np.percentile( df_merged['receita_incremental'], 75)
        # Definir as condições e os valores correspondentes
        conditions_ri = [
                df_merged['receita_incremental'] < receitaincremetal_25,  # Menor que o 1º quartil
                (df_merged['receita_incremental'] >= receitaincremetal_25) & (df_merged['receita_incremental'] < receitaincremetal_50),  # Entre 25% e 50%
                (df_merged['receita_incremental'] >= receitaincremetal_50) & (df_merged['receita_incremental'] < receitaincremetal_75),  # Entre 50% e 75%
                df_merged['receita_incremental'] >= receitaincremetal_75  # Maior ou igual ao 3º quartil
                    ]
        choices_ri = [1,2, 3, 4]
        df_merged['receitaincremental_quartis'] =  np.select(conditions_ri, choices_ri)



        #######################
        #  #classificando em grupo finais

        conditions_grupo = [
                #bloco vermelho    
                (df_merged['revenue_without_shipping_quartis'] >= 2) &  (df_merged['tendencia_quartis'] == -3 ),
                (df_merged['revenue_without_shipping_quartis'] == 3) &  (df_merged['tendencia_quartis'] == -2 ),
                #bloco verde  
                (df_merged['revenue_without_shipping_quartis'] >= 2) &  (df_merged['tendencia_quartis'] == 3 ), 
                (df_merged['revenue_without_shipping_quartis'] == 3) &  (df_merged['tendencia_quartis'] == 2 ),
                
                #bloco amarelo 
                (df_merged['revenue_without_shipping_quartis'] == 1) &  (df_merged['tendencia_quartis'] == -3 ),
                (df_merged['revenue_without_shipping_quartis'] == 2) &  (df_merged['tendencia_quartis'] == -2 ), 
                (df_merged['revenue_without_shipping_quartis'] == 3) &  (df_merged['tendencia_quartis'] == -1 ), 
                #bloco azul
                (df_merged['revenue_without_shipping_quartis'] == 3) &  (df_merged['tendencia_quartis'] == 1 ),
                (df_merged['revenue_without_shipping_quartis'] == 2) &  (df_merged['tendencia_quartis'] == 2 ), 
                (df_merged['revenue_without_shipping_quartis'] == 1) &  (df_merged['tendencia_quartis'] == 3 ),
                
                 ]
        choices_grupo = [-2,-2,2,2,-1,-1,-1,1,1,1]
        df_merged['grupos_finais'] =  np.select(conditions_grupo, choices_grupo)    

        df_merged['grupos_finais']=df_merged['grupos_finais'].fillna(0)


        
        grafico_dispersao(f"{diretorio}/positivo_grupo2{celular}.png",'Análise grupo verde',df_merged.loc[df_merged['grupos_finais'] == 2, ['revenue_without_shipping', 'rating_tendencia', 'tickemedio_quartis', 'grupos_finais', 'idprod']])
        grafico_dispersao(f"{diretorio}/positivo_grupo1{celular}.png",'Análise grupo azul',df_merged.loc[df_merged['grupos_finais'] == 1, ['revenue_without_shipping', 'rating_tendencia', 'tickemedio_quartis', 'grupos_finais', 'idprod']])
        grafico_dispersao(f"{diretorio}/negativo_grupo1{celular}.png",'Análise grupo amarelo',df_merged.loc[df_merged['grupos_finais'] == -1, ['revenue_without_shipping', 'rating_tendencia', 'tickemedio_quartis', 'grupos_finais', 'idprod']] )
        grafico_dispersao(f"{diretorio}/negativo_grupo2{celular}.png",'Análise grupo vermelho',df_merged.loc[df_merged['grupos_finais'] == -2, ['revenue_without_shipping', 'rating_tendencia', 'tickemedio_quartis', 'grupos_finais', 'idprod']])



        df_merged= df_merged.sort_values(by='score_final', ascending=True).reset_index(drop=True)
        
        df_primeiros_30 = df_merged.iloc[:43].copy()
        df_primeiros_30['ID-Nome'] = df_primeiros_30.apply(
                        lambda row: f"{row['idprod']}-{row['namesku_x']}", axis=1
                        )
          
        df_primeiros_30['Faturamento (6 meses)']  = df_primeiros_30.apply(
                        lambda row: f"R$ {abreviar_eixoy_moeda(row['revenue_without_shipping'],0,0,0)} ({abreviar_eixoy_moeda(row['percentualtotal']*100,1,0,0)}% repr.)", axis=1
                        )
        
        df_primeiros_30['Ticket Médio'] = df_primeiros_30['tickemedio_quartis'].apply(calcular_ticket_medio)
        df_primeiros_30['Receita Incremental'] = df_primeiros_30['receitaincremental_quartis'].apply(calcular_receita_incremental)
        df_primeiros_30['Tendência']  = df_primeiros_30['tendencia_quartis'].apply(calcular_tendencia)
       
        #print( df_primeiros_30['Receita Incremental'])
        tabela_detalhada(f"{diretorio}/tabela_detalhada{celular}.png",df_primeiros_30[['ID-Nome','Faturamento (6 meses)','Ticket Médio','Receita Incremental','Tendência','grupos_finais']])

        return  qtd_prod,qtd_prod_total


    except Exception as e:
        logging.error(f"erro ao processar os png : {e}")
        raise  # Ensure the Airflow task fails on error
        





def grafico_dispersao(nm_imagem,titulo,df_grafico):
   
    df_grafico = df_grafico.reset_index(drop=True)
    # Exemplo de dados
    x = df_grafico['revenue_without_shipping']   # Valores no eixo X
    y = df_grafico['rating_tendencia']   # Valores no eixo Y
    tamanho_bola = df_grafico['tickemedio_quartis']*100
    grupos = df_grafico['grupos_finais']
    ids = df_grafico['idprod']  # IDs dos produtos a serem anotados
    tipo_grupos = grupos[1] 
    #print(tipo_grupos)

   # print(tamanho_bola)
    colora = '#0a5c0a'
    colorb = '#1469b8'
    colorc = '#696969'
    colord = '#eead2d'
    colore=  '#8b0000'

    cores = grupos.map({2: colora, 1: colorb, 0: colorc, -1: colord, -2: colore})

    fig, ax = plt.subplots(figsize=(10, 8))  # Tamanho definido para a figura
    # Criando o gráfico de dispersão
    ax.scatter(x, y, color=cores , s=tamanho_bola)

    ax.grid(True, linestyle='-', which='both', color='#676270', alpha=0.05)

    plt.xticks(color='#676270', fontsize=18)
    plt.yticks(color='#676270', fontsize=18)
    plt.xlabel('Faturamento (R$)',color ='#676270',fontsize=20)
    plt.ylabel('Tendência(%)',color ='#676270',fontsize=20)
    # Títulos e rótulos
    
    ticksx = ax.get_xticks()
    ax.set_xticks(ticksx)  # Definir explicitamente os ticks do eixo Y
    valores_ajustado_x = [ abreviar_eixoy_moeda(x,0,0,1) for x in ticksx]
    ax.set_xticklabels(valores_ajustado_x)

    ticks = ax.get_yticks()
    ax.set_yticks(ticks)  # Definir explicitamente os ticks do eixo Y
    valores_ajustado = [ abreviar_eixoy_moeda(x*100,0,0,0) for x in ticks]
    ax.set_yticklabels(valores_ajustado)


    # Removendo as cores das bordas dos eixos x e y
    ax.spines['bottom'].set_color('none')
    ax.spines['left'].set_color('none')
    ax.spines['right'].set_color('none')
    ax.spines['top'].set_color('none')
    

    # Removendo os traços dos ticks dos eixos
    plt.tick_params(axis='x', length=0)  # Remove os ticks do eixo X
    plt.tick_params(axis='y', length=0)  # Remove os ticks do eixo Y

    if(tipo_grupos == -2 or tipo_grupos ==2):
        # Adicionar anotações para cada ponto dentro da bola
        for i in range(len(df_grafico)):
            ax.annotate(ids[i], (x[i], y[i]), textcoords="offset points", xytext=(0, 10),
                        ha='center',va= 'bottom', fontsize=14, color='black', weight='bold')
    else:
             # Adicionar anotações para cada ponto dentro da bola
        for i in range(len(df_grafico)):
            ax.annotate(ids[i], (x[i], y[i]), textcoords="offset points", xytext=(0, 10),
                        ha='center',va= 'bottom', fontsize=14, color='black', weight='bold')


    fig.suptitle(titulo, fontsize=22, fontweight='bold', ha='center',color="#15131B", y=0.98 )
    fig.subplots_adjust(top=0.8,left=0.12,right=0.95  )
    fancy_box = patches.FancyBboxPatch((0.03, 0.03), width=0.94, height=0.94,
                        boxstyle="round,pad=0.02,rounding_size=0.02", ec="lightgrey", lw=0.3, facecolor="none",
                        transform=fig.transFigure, clip_on=False)

    fig.patches.append(fancy_box)

    # Salvar a imagem
    plt.savefig(nm_imagem, dpi=300,bbox_inches='tight', pad_inches=0)
    plt.close()
    plt.show()


# Função para transformar valores numéricos em símbolos visuais
def calcular_ticket_medio(ticket):
    if ticket == 4:
        return '★★★★'
    elif ticket == 3:
        return '★★★☆'
    elif ticket == 2:
        return '★★☆☆'
    else:
        return '★☆☆☆'

def calcular_receita_incremental(receita):
    if receita == 4:
        return f"\\$\\$\\$\\$"
    elif receita == 3:
        return f"\\$\\$\\$"
    elif receita == 2:
        return f"\\$\\$"
    else:
        return f"\\$"



def calcular_tendencia(tendencia):
    if tendencia == 3:
        return '⬆️⬆️⬆️'
    elif tendencia == 2:
        return '⬆️⬆️'
    elif tendencia == 1:
        return '⬆️'
    elif tendencia == -3:
        return '⬇️⬇️⬇️'
    elif tendencia == -2:
        return '⬇️⬇️'
    elif tendencia == -1:
        return '⬇️'
    else:
        return '-'



def tabela_detalhada(nm_imagem,dataframe):

    # Register the DejaVu Sans font to render emojis
    plt.rcParams['font.family'] = 'DejaVu Sans'

    dataframe=dataframe.reset_index(drop=True)
    # Create a DataFrame
    df = dataframe.drop(columns=['grupos_finais'])
    
    #lagura entre as linhas 50 px e 300dpi 
    # Plotting the table using matplotlib
    fig, ax = plt.subplots(figsize=(1, 2))   # Adjust size according to your needs
    ax.axis('off')  # Hide axes

    # Create the table in the figure
    table = ax.table(cellText=df.values, colLabels=df.columns, cellLoc='center', loc='center')


    # Styling the table
    table.auto_set_font_size(False)
    table.set_fontsize(12)
    table.scale(1, 2.5)  # Adjust scale to make the table fit nicely
    
    #Personalizar a aparência de cada célula
    for (i, j), cell in table.get_celld().items():
        if i == 0:  # Linha de cabeçalho
            cell.set_fontsize(14)
            cell.set_text_props(weight='bold')
            cell.set_facecolor('#f4f4f4')  # Fundo cinza claro para o cabeçalho
            cell.set_text_props(ha='center')  # Cabeçalho centralizado

        # Alinhar a primeira coluna à esquerda (ID-Nome)
        if j == 0 and i > 0:  # Verificar se é a primeira coluna
            
            id_part, name_part = cell.get_text().get_text().split('-', 1)  # Separar ID e Nome
            grupo_valor = dataframe.iloc[i - 1]['grupos_finais']  # Obter valor do grupo para a linha

            # Definir cor com base no grupo (1 = verde, 2 = vermelho)
            if grupo_valor == -2:
                id_color = '#8b0000'
            elif grupo_valor == -1:
                id_color = '#eead2d'
            elif grupo_valor == 1:
                id_color = '#1469b8'
            elif grupo_valor == 2:
                id_color = '#0a5c0a'
            
            else:
                id_color = '#696969'  # Cor padrão

            # Definir o novo texto com o ID colorido e o nome em preto
            cell.set_fontsize(12)
            cell_text = f"{id_part}-{name_part}"
            cell.set_text_props(ha='left')  # Alinhar à esquerda
            cell.get_text().set_text(cell_text)
            cell.get_text().set_color(id_color if j == 0 else 'black') 

        if j == 1 and i > 0:  # faturamento 
            cell.set_fontsize(12)
            cell.set_text_props(color='black')

        if j == 2 and i > 0:  # Avaliações de Ticket Médio
            cell.set_fontsize(12)
            cell.set_text_props(color='#47346a')
        if j == 3 and i > 0:  # Avaliações de Receita Incremental
            cell.set_fontsize(12)
            cell.set_text_props(color='#0a5c0a')
        if j == 4 and i > 0:  # Avaliações de Tendência
            if '⬆️' in cell.get_text().get_text():  # Verificar se tem setas para cima
                cell.set_text_props(color='#00913f')
            elif '⬇️' in cell.get_text().get_text():  # Verificar se tem setas para baixo
                cell.set_text_props(color='#B2003B')

            
                

    table.auto_set_column_width([0,1,2,3,4])  #autoajuste para faturamento,ticket medio, receita incremental e tendencia

  #  table.auto_set_column_width([1,2,3,4])  #autoajuste para faturamento,ticket medio, receita incremental e tendencia

    # # Display the saved image using PIL
    # image = Image.open('styled_table_image.png')
    # image.show()
    plt.savefig(nm_imagem, dpi=300,bbox_inches='tight', pad_inches=0)




def gerar_pdf_analise(celular,integration,extensao,diretorio,caminho_pdf_blob):
   
    qtd_prod,qtd_prod_total=getbase(celular,integration,diretorio)

    pdf = FPDF(format='A4')
    pdf.add_page()

    # Obter a data de hoje
    data_hoje = datetime.datetime.now()
    # Subtrair um dia para obter o último dia do mês anterior
    ultimo_dia_mes_passado = data_hoje.replace(day=1) - datetime.timedelta(days=1)
    # Formatar a data como dd/mm/yyyy
    data_inicio =ultimo_dia_mes_passado.replace(day=1) - relativedelta(months=6)

    data_inicio_analise = data_inicio.strftime('%d/%m/%Y')

    data_formatada = ultimo_dia_mes_passado.strftime('%d/%m/%Y')
    
    # pdf.add_font('Helvetica', '', 'Fonte/Open_Sans/static/Helvetica-Regular.ttf')
    pdf.set_font('Helvetica', '', size=18)
    pdf.cell(0, 5, f"Análise da Loja Provel", align='C')
    pdf.ln(7)

    #colocando logo do lado esquerdo
    pdf.image(f"{diretorio}/logo_{celular}{extensao}", x = 15, y = 8 , w = 15,h=15)
    #colocando logo do lado direito
    pdf.image(f"{diretorio}/logo_{celular}{extensao}", x = 180, y = 8 , w = 15,h=15)

    mes_analise = f"Período de analise: {data_inicio_analise}  até {data_formatada}"

    pdf.set_font('Helvetica', '', size=10)
    pdf.cell(0, 5, mes_analise, align='C')
    pdf.ln(10)

        # Configurar a fonte
    pdf.set_font('Helvetica', '', 10)

    # Adicionar o texto principal
    texto_intro = f"A inteligência analisou um portfólio de {qtd_prod_total} produtos identificando os itens de maior impacto no faturamento da loja. A curva A contém {qtd_prod} produtos, dos quais analisamos suas respectivas representatividades no faturamento total, tendências de crescimento e queda, ticket médio e correlação com os outros produtos do portfólio. Com base nessa análise, classificamos os produtos de maior relevância em quatro categorias, que destacamos a seguir:"
    
    
    #dado mais completo
    #Com base em um portfólio de 150 produtos, realizamos uma análise utilizando a metodologia de curva ABC para identificar os itens de maior impacto no seu negócio. A curva A contém X produtos, e a partir dela calculamos a representatividade de cada um, analisamos as tendências de crescimento e também avaliamos o ticket médio de cada produto. Com isso, classificamos os produtos em quatro grupos principais:'''
    
    pdf.multi_cell(0, 6, texto_intro)
    # Inserir um espaço
    pdf.ln(4)
    verde_texto = "1 - Verde: SKUs cuja representatividade no faturamento total é bastante significativa e com tendência de crescimento acentuado. A recomendação é manter a estratégia atual, monitorar os níveis de estoque e replicar boas práticas para outros SKUs de pior desempenho."
     # Adicionar a descrição normal
    pdf.set_font('Helvetica', '', 8)
    pdf.multi_cell(0, 4, verde_texto)
   
    pdf.ln(2)
    vermelho_texto = "2 - Vermelho: SKUs cuja representatividade no faturamento total é bastante significativa e com tendência de queda acentuada. A recomendação é buscar formas de alavancar as vendas desses produtos, uma vez que sua queda resulta em alto impacto financeiro."
     # Adicionar a descrição normal
    pdf.set_font('Helvetica', '', 8)
    pdf.multi_cell(0, 4, vermelho_texto)
    pdf.ln(95)

    pdf.image(f"{diretorio}/positivo_grupo2{celular}.png", x = 7, y = 80, w = 95)
    pdf.image(f"{diretorio}/negativo_grupo2{celular}.png", x = 108, y =80, w = 95)

    azul_texto = "3 - Azul: SKUs de alta representatividade financeira e leve tendência de crescimento ou média representatividade financeira e forte tendência de crescimento. A estratégia atual deve ser mantida com monitoramento contínuo, assim como uma boa gestão de seus níveis de estoque."
     # Adicionar a descrição normal
    pdf.set_font('Helvetica', '', 8)
    pdf.multi_cell(0, 4, azul_texto)
    pdf.ln(2)
    amarelo_texto = "4 - Amarelo: SKUs de alta representatividade financeira e leve tendência de queda ou média representatividade financeira e forte tendência de crescimento. Verificar se houve alguma ruptura de estoque ou formas de alavancar as vendas dessas produtos."
     # Adicionar a descrição normal
    pdf.set_font('Helvetica', '', 8)
    pdf.multi_cell(0, 4, amarelo_texto)
    
    pdf.image(f"{diretorio}/positivo_grupo1{celular}.png", x = 7, y =190, w = 95)
    pdf.image(f"{diretorio}/negativo_grupo1{celular}.png", x = 108, y =190 , w = 95)
    
    pdf.add_page()
    # pdf.add_font('Helvetica', '', 'Fonte/Open_Sans/static/Helvetica-Regular.ttf')
    pdf.set_font('Helvetica', '', size=18)
    pdf.cell(0, 5, f"Análise da Loja Provel", align='C')
    pdf.ln(5)

            #colocando logo do lado esquerdo
    pdf.image(f"{diretorio}/logo_{celular}{extensao}", x = 15, y = 8 , w = 15,h=15)
    #colocando logo do lado direito
    pdf.image(f"{diretorio}/logo_{celular}{extensao}", x = 180, y = 8 , w = 15,h=15)

    mes_analise = f"Período de analise: {data_inicio_analise}  até {data_formatada}"

    pdf.set_font('Helvetica', '', size=8)
    pdf.cell(0, 5, mes_analise, align='C')
    pdf.ln(10)


        # Configurar a fonte
    pdf.set_font('Helvetica', '', 10)

    # Adicionar o texto principal
    texto_intro2 = f"Abaixo você encontra uma lista mais detalhada dos 43 SKUs que julgamos de maior relevância, após minuciosa análise de suas representatividades no faturamento total, ticket médio de seus pedidos, receita incremental de venda conjunta com outros SKUs e sua tendência recente."
    
    pdf.multi_cell(0, 6, texto_intro2)
    # Inserir um espaço
    pdf.ln(215)

    pdf.image(f"{diretorio}/tabela_detalhada{celular}.png", x = 7,y= 50,h=300,w=195 )

  
        # Configurar a fonte
    pdf.set_font('Helvetica', '', 6)
    # Adicionar o texto principal
    texto_intro3 = "Faturamento (6 meses): Faturamento do SKU sem frente acumulado dos últimos 6 meses.\nTicket Médio: Faturamento do pedido sem frente / total de pedidos com o SKU.\n\t\t\t\t\t 1*: Baixo Ticket médio; 2*: Médio TM; 3*: TM acima da média; 4*: Alto TM.\nReceita Incremental: Receita de outros SKUs no mesmo pedido.\n\t\t\t\t\t 1$: Muito baixo; 2$: baixo; 3$: médio; 4$: alto.\nTendência: Análise da tendência do faturamento sem frente dos últimos meses e semanas de cada SKU."
    
    pdf.multi_cell(0, 3, texto_intro3)
    
    filename= f"{caminho_pdf_blob}.pdf"
    pdf.output(f"{diretorio}/{filename}")

   
    return filename




def get_logo(logo,celular, diretorio):


    if(logo == ""):
        extensao = '.png'
        ExecuteBlob().get_file("appgemdata","teams-pictures/Logo_GD_preto.png",f"{diretorio}/logo_{celular}{extensao}") 
       
    else:   
            start_index = logo.rfind(".")
            extensao = logo[start_index:] 
            ExecuteBlob().get_file("appgemdata",logo,f"{diretorio}/logo_{celular}{extensao}") 
       
    return extensao


def salvar_pdf_blob(idintegration,diretorio,filename):
        try:
            ExecuteBlob().upload_file("jsondashboard",f"{idintegration}/report/{filename}",f"{diretorio}/{filename}") 
            print ("PDF gravado no blob com sucesso") 
        except Exception as e:
            print (f"erro ao gravar PDF mensal {e}") 


def criar_pasta_temp(celular):
    #Gera um UUID para criar um diretório único
    unique_id = uuid.uuid4().hex
    temp_dir = f"/opt/airflow/temp/{unique_id}_{celular}"

    # Cria o diretório
    os.makedirs(temp_dir, exist_ok=True)

    return temp_dir

    # Exemplo de caminho para salvar arquivos dentro do diretório temporário
    #temp_file_path = os.path.join(temp_dir, 'imagem.png')




def set_globals(data_conection,api_info,celphone,cami_logo,caminho_pdf,**kwargs):
    global  data_conection_info,idintegration ,celular,logo,caminho_pdf_blob
    data_conection_info = data_conection
    idintegration = api_info
    celular= celphone
    logo= cami_logo
    caminho_pdf_blob = caminho_pdf

    if not all([idintegration,celular]):
        logging.error("Global connection information is incomplete.")
        raise ValueError("All global connection information must be provided.")

    diretorio=criar_pasta_temp(celular)
    print(diretorio)

    extensao=get_logo(logo,celular,diretorio)
    filename=gerar_pdf_analise(celular,idintegration,extensao,diretorio,caminho_pdf_blob)

    salvar_pdf_blob(idintegration,diretorio,filename)
    
    # Remover o diretório após o uso
    shutil.rmtree(diretorio, ignore_errors=True)
    print(f"Diretório temporário {diretorio} removido.")




