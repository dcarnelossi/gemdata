import pandas as pd
from teste_dbpgconn import WriteJsonToPostgres
import logging
import numpy as np



def getbase(data_conection_info,schema):
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

                    from "{schema}".orders_items_ia
                    where
                    date_trunc('month',COALESCE(creationdate, '1900-01-01'))::date >= date_trunc('month', CURRENT_DATE) - INTERVAL '7 month'
                    and 
                    date_trunc('month',COALESCE(creationdate, '1900-01-01'))::date <= date_trunc('month', CURRENT_DATE) - INTERVAL '1 month'
                   
                    group by 1,2 


                    --limit 3

                    """
       
        
        _, result = WriteJsonToPostgres("integrations-data-dev", query, "orders_items_ia").query()
        if not result:
            logging.warning("No skus found in the database.")
            return False
        

        query_pedidos =f""" 
					select 
                    cast(count(distinct orderid) as int) as pedidos
                    from "{schema}".orders_items_ia
                    where
                    date_trunc('month',COALESCE(creationdate, '1900-01-01'))::date >= date_trunc('month', CURRENT_DATE) - INTERVAL '6 month'
                    and 
                    date_trunc('month',COALESCE(creationdate, '1900-01-01'))::date <= date_trunc('month', CURRENT_DATE) - INTERVAL '1 month'
                   
                    """
       
        
        _, result_pedidos = WriteJsonToPostgres("integrations-data-dev", query_pedidos, "orders_items_ia").query()
        if not result_pedidos:
            logging.warning("No skus found in the database.")
            return False

        df_pedidos = pd.DataFrame(result_pedidos)    


        
        #print(result) 
        #df_tupla = pd.DataFrame(result, columns=['idprod', 'namesku', 'revenue_without_shipping', 'pedidos', 'revenue_orders', 'tickemedio', 'receita_incremental'])
        df_ticket = pd.DataFrame(result)
        
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
                        from "{schema}".orders_items_ia b 
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
	                	
                       from "{schema}".orders_items_ia o
                       left join data_base d on
                       d.orderid = o.orderid
                   
                    where
                    date_trunc('month',COALESCE(creationdate, '1900-01-01'))::date >= dataini
                    and 
                    date_trunc('month',COALESCE(creationdate, '1900-01-01'))::date <= datafim 
                  
                    group by 1,2 ;

                    """
       
        
        _, tend = WriteJsonToPostgres("integrations-data-dev", query_item, "orders_items_ia").query()
        if not result:
            logging.warning("Sem resultado da segunda query ")
            return False

        #print(result) 
        #df_tupla = pd.DataFrame(result, columns=['idprod', 'namesku', 'revenue_without_shipping', 'pedidos', 'revenue_orders', 'tickemedio', 'receita_incremental'])
        df_tendencia = pd.DataFrame(tend)


    
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
       (df_tendencia_curvaa['revenue_12semanas']/df_tendencia_curvaa['revenue_8semanas'])-1 # Caso contrário, calcular a variação
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
    
        df_merged.to_excel('xxxdados_teste.xlsx', index=False)


    except Exception as e:
        logging.error(f"An error occurred in get_categories_id_from_db: {e}")
        raise  # Ensure the Airflow task fails on error
    
getbase("integrations-data-dev","2dd03eaf-cf56-4a5b-bc99-3a06b237ded8")