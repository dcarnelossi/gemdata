import logging
from teste_dbpgconn import WriteJsonToPostgres
import pandas as pd 
from datetime import datetime
import psycopg2
import orjson, json
from collections import defaultdict



def base_exportar_order(schema):
   
    # Consulta para puxar os dados
    query_consulta = f""" 
        SELECT * FROM "5e164a4b-5e09-4f43-9d81-a3d22b09a01b".realtime_forecast_hour 


    """
   # print(query_consulta)

    # Executa a consulta e obtém os resultados
    _,results_named = WriteJsonToPostgres("integrations-data-prod", query_consulta, "realtime_forecast_hour").query()

    # df = pd.DataFrame(results_named)
    # df.to_csv('faturamento_categoria3.csv',sep='|', index=False, encoding='utf-8') 
    return results_named
    


def base_exportar_order2(schema):
   
    # Consulta para puxar os dados
    query_consulta = f""" 
       
		
				
				WITH realtime_orders AS (
                select 
                 to_char(date_trunc('hour',o.createdat AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' ), 'YYYY-MM-DD') as creationdate  
                 , EXTRACT(YEAR FROM date_trunc('hour',o.createdat AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' ))::int                    AS ano
				 , EXTRACT(HOUR FROM date_trunc('hour',o.createdat AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' ))::int                    AS hora
                ,o.orderid as orderid
                ,LOWER(o.displayfinancialstatus) as statusdescription
                ,cast(1 as float) as quantityorder
                ,cast(o.totalprice as float)  as revenue
                ,(cast(o.totalprice as float))-(cast(o.totalshippingprice as float))   as revenue_without_shipping
                ,LOWER(coalesce(nome_codigo_municipio_completo,coalesce(o.shippingcity,o.billingcity))) as selectedaddresses_0_city
                ,LOWER(coalesce(abreviado_uf, coalesce(o.shippingprovincecode,o.billingprovincecode))) as selectedaddresses_0_state
                ,LOWER(coalesce(o.shippingcountrycode,o.billingcountrycode)) as selectedaddresses_0_country
                ,case when cast(o.totalshippingprice as numeric) =0 then  'Sem Frete' else  'Com Frete' end  FreeShipping 
                
                from "5e164a4b-5e09-4f43-9d81-a3d22b09a01b".realtime_shopify_orders o               
                left join public.cep_brasil_consolidado ce on 
                ce.cep = cast(NULLIF(left(coalesce(REPLACE(o.shippingzip,'-',''),REPLACE(o.billingzip,'-','')),5), '') as int)   
                
                where 
                to_char(date_trunc('day',o.createdat AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' ), 'YYYY-MM-DD') = to_char(date_trunc('day',CURRENT_DATE AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' ), 'YYYY-MM-DD') 
                
                ), final_hora as ( 
				select
					*
				from realtime_orders 
				
				union all 
				
				select * from "5e164a4b-5e09-4f43-9d81-a3d22b09a01b".realtime_orders_lastyear
				)
				select *,
				to_char((CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo' ), 'DD/MM/YYYY HH24:MI') as  hora_atualizacao
				from final_hora
			
				
    



    """
   # print(query_consulta)

    # Executa a consulta e obtém os resultados
    _,results_named = WriteJsonToPostgres("integrations-data-prod", query_consulta, "realtime_shopify_orders").query()

    # df = pd.DataFrame(results_named)
    # df.to_csv('faturamento_categoria3.csv',sep='|', index=False, encoding='utf-8') 
    return results_named
    

json_ar2=base_exportar_order("5e164a4b-5e09-4f43-9d81-a3d22b09a01b")

# Salvar o JSON em um arquivo
with open("realtime_forecast_hour.json", "w", encoding="utf-8") as outfile:
    # Converte o objeto em uma string JSON formatada
    json.dump(json_ar2, outfile, ensure_ascii=False, indent=4)


print("JSON salvo em 'dados.json'")


json_ar3=base_exportar_order2("5e164a4b-5e09-4f43-9d81-a3d22b09a01b")

# Salvar o JSON em um arquivo
with open("realtime_orders.json", "w", encoding="utf-8") as outfile:
    # Converte o objeto em uma string JSON formatada
    json.dump(json_ar3, outfile, ensure_ascii=False, indent=4)


print("JSON salvo em 'dados.json'")

#PARA FAZER A TELA DE SAZONALIDADE 

# select 
# to_char(DATE_TRUNC('day',  creationdate) , 'YYYY-MM-DD')  as dt,
# EXTRACT(YEAR FROM ord.creationdate)::int                    AS ano,
# cast(orderid as varchar(100)) as ord ,
# cast(idprod as varchar(50)) as iprod,
# cast(namesku as varchar(200)) as nmsku,
# cast(idcat as varchar(50)) as idcat,
# cast(namecategory as varchar(200)) as nmcat,
# cast(quantityitems as INT) as qtdi,
# cast(revenue_without_shipping as  FLOAT) as fat ,
# cast(selectedaddresses_0_city as varchar(300)) as cid,
# cast(selectedaddresses_0_state as varchar(300))as est,
# cast(paymentnames as varchar(200)) as pay,
# cast(isfreeshipping as varchar(100)) as isship ,
# cast(dt_feriado  as varchar(100)) as dtf,
# cast(nm_feriado  as varchar(100)) as nmferi,
# cast(userprofileid  as varchar(100)) as user,
# cast((ord.creationdate::date - fer.dt_feriado::date ) as int) AS diff_dias

# FROM "{schema}".orders_items_ia           AS ord
# inner JOIN public.tb_forecast_feriado  AS fer
#   ON ord.creationdate::date BETWEEN (fer.dt_feriado::date - INTERVAL '15 days')
#                                AND (fer.dt_feriado::date + INTERVAL '7 days')




# SELECT 
#   to_char(date_trunc('day', ord.creationdate), 'YYYY-MM-DD') AS dt,  -- dayKey já em string
#   EXTRACT(YEAR FROM ord.creationdate)::int                    AS ano,
#   EXTRACT(HOUR FROM ord.creationdate)::int                    AS hora,
# cast(orderid as varchar(100)) as ord,
# cast(quantityitems as INT) as qtitem,
# cast(round(cast(revenue as NUMERIC(18,2)),2)as float) as fatf,
# cast(round(cast(revenue_without_shipping as NUMERIC(18,2)),2)as float)  as fat,
# cast(selectedaddresses_0_city as varchar(300)) as cid,
# cast(selectedaddresses_0_state as varchar(300))as est,
# cast(paymentnames as varchar(200)) as pay,
# cast(isfreeshipping as varchar(100)) as isship ,
# cast(dt_feriado  as varchar(100)) as dtf,
# cast(nm_feriado  as varchar(100)) as nmferi,
# cast(userprofileid  as varchar(100)) as user,
# cast((ord.creationdate::date - fer.dt_feriado::date ) as int) AS diff_dias



# FROM "{schema}".orders_ia            AS ord
# inner JOIN public.tb_forecast_feriado  AS fer
#   ON ord.creationdate::date BETWEEN (fer.dt_feriado::date - INTERVAL '15 days')
#                                AND (fer.dt_feriado::date + INTERVAL '7 days')




# select distinct 
#     cast( fer.nm_feriado as varchar(100) ) as nmf,
#     to_char(date_trunc('day', fer.dt_feriado), 'YYYY-MM-DD') as dtf,
#      to_char(date_trunc('day', (fer.dt_feriado + offs.offset_day)), 'YYYY-MM-DD') AS  dtref, 
#     cast( offs.offset_day as int) AS diffdias
# FROM public.tb_forecast_feriado AS fer
# CROSS JOIN LATERAL (
#     SELECT generate_series(-15, 7) AS offset_day
# ) AS offs
# WHERE EXTRACT(YEAR FROM fer.dt_feriado) >= EXTRACT(YEAR FROM CURRENT_DATE) - 3
#   AND EXTRACT(YEAR FROM fer.dt_feriado) <=  EXTRACT(YEAR FROM CURRENT_DATE) + 1;



#PARA FAZER O FORECAST IA 
# select distinct 
#     cast( fer.nm_feriado as varchar(100) ) as nmf,
#     to_char(date_trunc('day', fer.dt_feriado), 'YYYY-MM-DD') as dtf,
#      to_char(date_trunc('day', (fer.dt_feriado + offs.offset_day)), 'YYYY-MM-DD') AS  dtref, 
#     cast( offs.offset_day as int) AS diffdias
# FROM "5e164a4b-5e09-4f43-9d81-a3d22b09a01b".tb_forecast_feriado AS fer
# CROSS JOIN LATERAL (
#     SELECT generate_series(-15, 7) AS offset_day
# ) AS offs
# WHERE EXTRACT(YEAR FROM fer.dt_feriado) >= EXTRACT(YEAR FROM CURRENT_DATE) - 3
#   AND EXTRACT(YEAR FROM fer.dt_feriado) <=  EXTRACT(YEAR FROM CURRENT_DATE) + 1; 





##PARA FAZER A TELA DE REALTIME  
################

#PRIMEIRA PARTE QUE DEVE SER FEITO APENAS 1 VEZ NO DIA  - VIRA TABELA 
                # DROP TABLE IF EXISTS realtime_orders_lastyear;
                # CREATE TABLE realtime_orders_lastyear
                # as
				# select 
				# to_char(date_trunc('day', creationdate), 'YYYY-MM-DD') AS dt, 
				# EXTRACT(YEAR FROM creationdate)::int                    AS ano,
				# EXTRACT(HOUR FROM creationdate)::int                    AS hora,
				# orderid,
				# statusdescription,
				# quantityorder,
				# revenue,
				# revenue_without_shipping,
				# selectedaddresses_0_city,
				# selectedaddresses_0_state,
				# selectedaddresses_0_country,
				# FreeShipping 
				
				# from orders_ia  
				# 	where to_char(date_trunc('day', creationdate), 'YYYY-MM-DD') = to_char( (CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo')::date - INTERVAL '1 year', 'YYYY-MM-DD')
				
				
				
			# DROP TABLE IF EXISTS realtime_forecast_hour;
			# 	create table realtime_forecast_hour
			# 	as
			# 	WITH share_hour AS (
			# 		    SELECT
			# 			 	EXTRACT(HOUR FROM creationdate)::int     AS hora,
			# 			    SUM(revenue) / SUM(SUM(revenue)) OVER () AS share_revenue
			# 			FROM orders_ia
			# 			where date_trunc('day', creationdate) >=  date_trunc('day',(CURRENT_DATE - INTERVAL '3 month'))
			# 			and 
			# 			 date_trunc('day', creationdate) <  date_trunc('day',(CURRENT_DATE- INTERVAL '1 days'))
			# 			and 
			# 			 EXTRACT(DOW FROM creationdate) = EXTRACT(DOW FROM (CURRENT_DATE AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' ))
						
			# 			GROUP BY   EXTRACT(HOUR FROM creationdate)::int 
			# 		)
			# 		select
			# 			to_char(date_trunc('day', creationdateforecast), 'YYYY-MM-DD') as creationdateforecast,
			# 		    sh.hora::int   ,
			# 		   CEIL(cast( ff.predicted_revenue * sh.share_revenue AS float)) AS predicted_revenue_hour,
			# 		   CEIL(cast( (ff.predicted_revenue * sh.share_revenue)*1.15 AS float)) as max_predicted_revenue_hour,
			# 		   CEIL(cast( (ff.predicted_revenue * sh.share_revenue)*0.85 AS float)) as min_predicted_revenue_hour,
					   
			# 		    cast(   ff.predicted_revenue as FLOAT)                   AS predicted_revenue_day
			# 		FROM share_hour sh
			# 		CROSS JOIN orders_ia_forecast ff
			# 		WHERE 
			# 		to_char(date_trunc('day', creationdateforecast), 'YYYY-MM-DD') = to_char(date_trunc('day',(CURRENT_DATE AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' )), 'YYYY-MM-DD')	    
				
						    
#ULTIMA PARTE - VIRA ARQUIVO 	
				
				
				# WITH realtime_orders AS (
                # select 
                #  to_char(date_trunc('hour',o.createdat AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' ), 'YYYY-MM-DD') as creationdate  
                #  , EXTRACT(YEAR FROM date_trunc('hour',o.createdat AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' ))::int                    AS ano
				#  , EXTRACT(HOUR FROM date_trunc('hour',o.createdat AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' ))::int                    AS hora
                # ,o.orderid as orderid
                # ,LOWER(o.displayfinancialstatus) as statusdescription
                # ,cast(1 as float) as quantityorder
                # ,cast(o.totalprice as float)  as revenue
                # ,(cast(o.totalprice as float))-(cast(o.totalshippingprice as float))   as revenue_without_shipping
                # ,LOWER(coalesce(nome_codigo_municipio_completo,coalesce(o.shippingcity,o.billingcity))) as selectedaddresses_0_city
                # ,LOWER(coalesce(abreviado_uf, coalesce(o.shippingprovincecode,o.billingprovincecode))) as selectedaddresses_0_state
                # ,LOWER(coalesce(o.shippingcountrycode,o.billingcountrycode)) as selectedaddresses_0_country
                # ,case when cast(o.totalshippingprice as numeric) =0 then  'Sem Frete' else  'Com Frete' end  FreeShipping 
                
                # from "5e164a4b-5e09-4f43-9d81-a3d22b09a01b".realtime_shopify_orders o               
                # left join public.cep_brasil_consolidado ce on 
                # ce.cep = cast(NULLIF(left(coalesce(REPLACE(o.shippingzip,'-',''),REPLACE(o.billingzip,'-','')),5), '') as int)   
                
                # where 
                # to_char(date_trunc('day',o.createdat AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' ), 'YYYY-MM-DD') = to_char(date_trunc('day',CURRENT_DATE AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' ), 'YYYY-MM-DD') 
                
                # ), final_hora as ( 
				# select
				# 	*
				# from realtime_orders 
				
				# union all 
				
				# select * from "5e164a4b-5e09-4f43-9d81-a3d22b09a01b".realtime_orders_lastyear
				# )
				# select *,
				# to_char((CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo' ), 'DD/MM/YYYY HH24:MI') as  hora_atualizacao
				# from final_hora
			
				

    #TERCEIRA PARTE - VIRA ARQUIVO 
    # 	SELECT * FROM "5e164a4b-5e09-4f43-9d81-a3d22b09a01b".realtime_forecast_hour 