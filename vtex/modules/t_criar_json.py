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
  

select distinct 
    cast( fer.nm_feriado as varchar(100) ) as nmf,
    to_char(date_trunc('day', fer.dt_feriado), 'YYYY-MM-DD') as dtf,
     to_char(date_trunc('day', (fer.dt_feriado + offs.offset_day)), 'YYYY-MM-DD') AS  dtref, 
    cast( offs.offset_day as int) AS diffdias
FROM "5e164a4b-5e09-4f43-9d81-a3d22b09a01b".tb_forecast_feriado AS fer
CROSS JOIN LATERAL (
    SELECT generate_series(-15, 7) AS offset_day
) AS offs
WHERE EXTRACT(YEAR FROM fer.dt_feriado) >= EXTRACT(YEAR FROM CURRENT_DATE) - 3
  AND EXTRACT(YEAR FROM fer.dt_feriado) <=  EXTRACT(YEAR FROM CURRENT_DATE) + 1;
                               


    """
   # print(query_consulta)

    # Executa a consulta e obtém os resultados
    _,results_named = WriteJsonToPostgres("integrations-data-prod", query_consulta, "orders_ia").query()

    # df = pd.DataFrame(results_named)
    # df.to_csv('faturamento_categoria3.csv',sep='|', index=False, encoding='utf-8') 
    return results_named
    



json_ar2=base_exportar_order("5e164a4b-5e09-4f43-9d81-a3d22b09a01b")

# Salvar o JSON em um arquivo
with open("seasonality_holiday.json", "w", encoding="utf-8") as outfile:
    # Converte o objeto em uma string JSON formatada
    json.dump(json_ar2, outfile, ensure_ascii=False, indent=4)


print("JSON salvo em 'dados.json'")



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

# FROM "5e164a4b-5e09-4f43-9d81-a3d22b09a01b".orders_items_ia           AS ord
# inner JOIN "5e164a4b-5e09-4f43-9d81-a3d22b09a01b".tb_forecast_feriado  AS fer
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



# FROM "5e164a4b-5e09-4f43-9d81-a3d22b09a01b".orders_ia            AS ord
# inner JOIN "5e164a4b-5e09-4f43-9d81-a3d22b09a01b".tb_forecast_feriado  AS fer
#   ON ord.creationdate::date BETWEEN (fer.dt_feriado::date - INTERVAL '15 days')
#                                AND (fer.dt_feriado::date + INTERVAL '7 days')




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



# fazer o forecast ia 
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