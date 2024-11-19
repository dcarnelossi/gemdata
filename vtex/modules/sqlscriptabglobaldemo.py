
def vtexsqlscriptscreatetabglobaldemo(schema):
    scripts = f"""
                 
                 DROP TABLE IF EXISTS ordersfretegratis;
                CREATE TEMPORARY TABLE ordersfretegratis as
                select distinct  oi.orderid,case when cast(shipping as numeric) =0 then  'true' else  'false' end  isFreeShipping  
                from  "{schema}".orders_totals oi;  

                DROP TABLE IF EXISTS "{schema}".orders_items_ia;
                create TABLE "{schema}".orders_items_ia
                as
                select 
                 date_trunc('hour',ol.creationdate AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' ) as creationdate ,
                oi.orderid,
                cast(cast(coalesce(oi.sellersku,'999999') as integer)*2  as varchar(20)) as idprod ,
                CONCAT('Nome Produto ', cast(cast(coalesce(oi.sellersku,'999999') as integer)*2 as varchar(20)))  as namesku,
                cast(cast(coalesce(pro.categoryid,'999999') as integer)*2  as varchar(20))  as idcat,
                CONCAT('Nome Categoria ', cast(cast(coalesce(pro.categoryid,'999999') as integer)*2 as varchar(20))) as namecategory,
                oi.tax,
                oi.taxcode,
                cast(round(cast(cast(1 as float)* (1+(RANDOM() * (2)))as decimal),0) as float)  as quantityorder,
                cast(round(cast(cast(oi.quantity as decimal)* (3+(RANDOM() * (3)))as decimal),0) as float)  as quantityitems ,
                cast(round(cast((cast(oi.price as float)/100)* ROUND(cast(RANDOM()as decimal) * 3, 1) as decimal),2) as float) as price,
                cast(round(cast((cast(oi.costprice as float)/100)* ROUND(cast(RANDOM()as decimal) * 3, 1) as decimal),2) as float)as costprice,
                cast(round(cast((cast(oi.listprice as float)/100)* ROUND(cast(RANDOM()as decimal) * 3, 1) as decimal),2) as float)  as listprice,
                cast(round(cast((cast(oi.commission as float)/100)* ROUND(cast(RANDOM()as decimal) * 3, 1) as decimal),2) as float)  as commission,
                
                cast(round(cast((cast(oi.sellingprice as float)/100)* ROUND(cast(RANDOM()as decimal) * 3, 1) as decimal),2) as float)  as sellingprice,
                cast(round(cast(((cast(oi.price as float)/100) - (cast(oi.sellingprice as float)/100))* cast(oi.quantity as float) * ROUND(cast(RANDOM()as decimal) * 3, 1) as decimal),2) as float)  as totaldiscounts,
                 cast(round(cast((cast(oi.sellingprice as float)/100)* cast(oi.quantity as float)* ROUND(cast(RANDOM()as decimal) * 3, 1) as decimal) ,2) as float)  as revenue_without_shipping ,
                oi.isgift
                ,LOWER(sd.selectedaddresses_0_city) as selectedaddresses_0_city
                ,LOWER(sd.selectedaddresses_0_state) as selectedaddresses_0_state
                ,LOWER(sd.selectedaddresses_0_country) as selectedaddresses_0_country
                ,cp.userprofileid
                ,case when  LOWER(ol.paymentnames) = 'visa' then 'visa'
                when  LOWER(ol.paymentnames) = 'mastercard' then 'mastercard'
                when  LOWER(ol.paymentnames) = 'pix' then 'pix'
                else
                'outros'
                end as paymentnames
                --,oi.saleschannel as saleschannel
                ,LOWER(ol.statusdescription) as statusdescription
                ,'marketplace' as origin
                ,fg.isFreeShipping
                ,cast(round(cast(((cast(od.value as float)/100)-(cast(ot.shipping as float)/100) )* ROUND(cast(RANDOM()as decimal) * 3, 1) as decimal),2) as float)  as revenue_orders_out_ship


                from "{schema}".orders_items oi 

                inner join "{schema}".orders_list ol on 
                ol.orderid = oi.orderid

                left join "{schema}".skus sku on 
                sku.id = cast(oi.sellersku as int)

                left join "{schema}".products pro on 
                pro.id = sku.productid

                left join "{schema}".categories cat on 
                cat.id = pro.categoryid

                left join "{schema}".orders_shippingdata sd on 
                sd.orderid = oi.orderid

                left join "{schema}".client_profile cp on 
                cp.orderid = oi.orderid

                left join ordersfretegratis fg 
                on fg.orderid = oi.orderid

                left join "{schema}".orders od 
                on od.orderid = oi.orderid

                left join "{schema}".orders_totals ot on 
                ot.orderid = oi.orderid
                        
                where 
                LOWER(ol.statusdescription)  in  ('faturado','pronto para o manuseio');


                DROP TABLE IF EXISTS qtditemorder;
                CREATE TEMPORARY TABLE qtditemorder as
                select oi.orderid, cast(round(cast(sum(cast(oi.quantity as numeric)* (3+(RANDOM() * (3))))as decimal),0) as numeric) as quantityitems  
                from "{schema}".orders_items oi 
                group by orderid;



                DROP TABLE IF EXISTS "{schema}".orders_ia;

                create table "{schema}".orders_ia
                as
                select 
                date_trunc('hour',ol.creationdate AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' ) as creationdate  
                ,o.orderid
                ,'marketplace' as origin
                ,'1' as saleschannel
                ,LOWER(o.statusdescription) as statusdescription
                ,cast(round(cast(qt.quantityitems as decimal),0) as float) as quantityitems
                ,cast(round(cast((1+(RANDOM() * (2))) as decimal),0) as float) as quantityorder
                ,cast(round(cast((cast(ot.items as float)/100)*ROUND(cast(RANDOM()as decimal) * 3, 1) as decimal),2)as float) as itemsPriceDar 
                ,cast(round(cast((cast(ot.discounts as float)/100)*ROUND(cast(RANDOM()as decimal) * 3, 1) as decimal),2)as float) as DiscountsPrice
                ,cast(round(cast((cast(ot.shipping as float)/100)*ROUND(cast(RANDOM()as decimal) * 3, 1) as decimal),2)as float)  as ShippingPrice
                ,cast(round(cast((cast(ot.tax as float)/100)*ROUND(cast(RANDOM()as decimal) * 3, 1) as decimal),2)as float)  as tax
                ,cast(round(cast((cast(o.value as float)/100)*ROUND(cast(RANDOM()as decimal) * 3, 1) as decimal),2)as float)   as revenue
                ,cast(round(cast(((cast(o.value as float)/100)-(cast(ot.shipping as float)/100))*ROUND(cast(RANDOM()as decimal) * 3, 1) as decimal),2)as float)    as revenue_without_shipping
                ,case when  LOWER(ol.paymentnames) = 'visa' then 'visa'
                when  LOWER(ol.paymentnames) = 'mastercard' then 'mastercard'
                when  LOWER(ol.paymentnames) = 'pix' then 'pix'
                else
                'outros'
                end as paymentnames
                ,LOWER(sd.selectedaddresses_0_city) as selectedaddresses_0_city
                ,LOWER(sd.selectedaddresses_0_state) as selectedaddresses_0_state
                ,LOWER(sd.selectedaddresses_0_country) as selectedaddresses_0_country
                ,cp.userprofileid
                ,case when cast(ot.shipping as numeric) =0 then  'Sem Frete' else  'Com Frete' end  FreeShipping 
                ,case when cast(ot.shipping as numeric) =0 then  'true' else  'false' end  isFreeShipping 
                ,case when round( cast(random() * (1 - 0) as numeric),0)=0 then 'F' else 'M' end   as Sexo 


                from "{schema}".orders o 

                left join "{schema}".orders_totals  ot 
                    on ot.orderid= o.orderid

                left join "{schema}".orders_list ol on 
                ol.orderid = o.orderid

                left join "{schema}".orders_shippingdata sd on 
                sd.orderid = o.orderid

                left join "{schema}".client_profile cp on 
                cp.orderid = o.orderid

                left join qtditemorder qt on 
                qt.orderid = o.orderid

                where 
                LOWER(ol.statusdescription)  in  ('faturado','pronto para o manuseio');
                
       
                DO $$
                BEGIN
                   
                    IF EXISTS (
                        SELECT 1
                        FROM pg_catalog.pg_tables
                        WHERE schemaname = '{schema}'
                        AND tablename = 'orders_ia_forecast'
                    ) THEN
                        RAISE NOTICE 'A tabela forecast já existe. Código não será executado.';
                    else
                        CREATE TABLE "{schema}".orders_ia_forecast (
                                creationdateforecast timestamp NOT NULL,
                                predicted_revenue numeric NOT NULL,
                                CONSTRAINT constraint_orders_forecast UNIQUE (creationdateforecast)
                        );
                        
                                
                        DROP TABLE IF EXISTS realforecast;
                        CREATE TEMP TABLE realforecast AS (
                            SELECT 
                                creationdate::date AS creationdate,
                                SUM(revenue) AS revenue
                            FROM "{schema}".orders_ia
                            GROUP BY creationdate::date
                        );
                        
                        WITH faturamento_base AS (
                            SELECT
                                creationdate as data,
                                revenue as faturamento,
                                EXTRACT(DOW FROM creationdate) AS dia_semana  -- Extrai o dia da semana (0 = domingo, 6 = sábado)
                            FROM
                                realforecast
                        ),
                        ajuste_sazonal AS (
                            SELECT
                                dia_semana,
                                AVG(faturamento) AS ajuste_sazonal
                            FROM
                                faturamento_base
                            GROUP BY
                                dia_semana
                        ),
                        datas_futuras AS (
                            SELECT
                                generate_series(
                                    (SELECT MAX(data) + INTERVAL '1 day' FROM faturamento_base),
                                    (SELECT DATE_TRUNC('month', MAX(data)) + INTERVAL '2 months' - INTERVAL '1 day' FROM faturamento_base),
                                    '1 day'::interval
                                ) AS data
                        ),
                        datas_com_sazonalidade AS (
                            SELECT
                                d.data,
                                EXTRACT(DOW FROM d.data) AS dia_semana
                            FROM
                                datas_futuras d
                        )

                        insert into "{schema}".orders_ia_forecast
                        SELECT
                            dcs.data,
                            asz.ajuste_sazonal AS faturamento_projetado
                        FROM
                            datas_com_sazonalidade dcs
                        LEFT JOIN
                            ajuste_sazonal asz
                        ON
                            dcs.dia_semana = asz.dia_semana
                        ORDER BY
                            dcs.data;
                        
                    END IF;
                END $$;

                

    """
    # print(scripts)
    return scripts

def vtexsqlscriptscreatetabglobaldemocopy(schemacopy,schema):
    scripts = f"""
                                        
                        CREATE SCHEMA IF NOT EXISTS "{schema}";

                        DROP TABLE IF EXISTS "{schema}".orders_items_ia;
                        create TABLE "{schema}".orders_items_ia
                        as
                        SELECT 
                        orderid,
                        creationdate,
                        cast(cast(idcat as integer)*2  as varchar(20)) as idcat,
                        CONCAT('Nome Categoria ', cast(cast(idcat as integer)*2 as varchar(20))) as namecategory,
                        cast(cast(idprod as integer)*2  as varchar(20))  as idprod,   
                        CONCAT('Nome Produto ', cast(cast(idprod as integer)*2 as varchar(20))) as namesku,
                        sellingprice * (1.05 + (RANDOM() * (0.20))) as sellingprice ,
                        quantityorder * (2 + (RANDOM() * (0.20))) as quantityorder,
                        quantityitems * (2 + (RANDOM() * (0.20))) as quantityitems,
                        revenue_without_shipping * (1.05 + (RANDOM() * (0.20))) as revenue_without_shipping,
                        selectedaddresses_0_city,
                        selectedaddresses_0_state
                        FROM "{schemacopy}".orders_items_ia  ;
                        

                        DROP TABLE IF EXISTS "{schema}".orders_ia;
                        create TABLE "{schema}".orders_ia
                        as
                        SELECT 
                        orderid,
                        creationdate,
                        userprofileid,
                        revenue * (1.05 + (RANDOM() * (0.20))) as revenue,
                        quantityorder * (2 + (RANDOM() * (0.20))) as quantityorder,
                        selectedaddresses_0_state,
                        selectedaddresses_0_city

                        FROM "{schemacopy}".orders_ia  ;
                        
                        

                

    """
    # print(scripts)
    return scripts
