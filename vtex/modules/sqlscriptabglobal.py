def vtexsqlscriptscreatetabglobal(schema):
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
                coalesce(oi.sellersku,'999999') as idprod ,
                LOWER(coalesce(sku.namecomplete,'Não informado')) as namesku,
                coalesce(pro.categoryid,'999999') as idcat,
                LOWER(coalesce(cat.name,'Não informado')) as namecategory,
                oi.tax,
                oi.taxcode,
                cast(1 as float) as quantityorder,
                cast(oi.quantity as float) as quantityitems ,
                cast(oi.price as float)/100 as price,
                cast(oi.costprice as float)/100  as costprice,
                cast(oi.listprice as float)/100 as listprice,
                cast(oi.commission as float)/100 as commission,
                --cast(oi.shippingprice as numeric)/100 as shippingprice,
                cast(oi.sellingprice as float)/100 as sellingprice,
                ((cast(oi.price as float)/100) - (cast(oi.sellingprice as float)/100))* cast(oi.quantity as float)  as totaldiscounts,
                (cast(oi.sellingprice as float)/100)* cast(oi.quantity as float)  as revenue_without_shipping ,
                oi.isgift
                ,LOWER(sd.selectedaddresses_0_city) as selectedaddresses_0_city
                ,LOWER(sd.selectedaddresses_0_state) as selectedaddresses_0_state
                ,LOWER(sd.selectedaddresses_0_country) as selectedaddresses_0_country
                ,cp.userprofileid
                ,LOWER(ol.paymentnames) as paymentnames
                --,oi.saleschannel as saleschannel
                ,LOWER(ol.statusdescription) as statusdescription
                ,LOWER(ol.origin) as origin
                ,fg.isFreeShipping
                ,(cast(od.value as float)/100)-(cast(ot.shipping as float)/100) as revenue_orders_out_ship


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
                select oi.orderid, sum(cast(oi.quantity as numeric)) as quantityitems  
                from "{schema}".orders_items oi 
                group by orderid;



                DROP TABLE IF EXISTS "{schema}".orders_ia;

                create table "{schema}".orders_ia
                as
                select 
                date_trunc('hour',ol.creationdate AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' ) as creationdate  
                ,o.orderid
                ,LOWER(o.origin) as origin
                ,LOWER(O.saleschannel) as saleschannel
                ,LOWER(o.statusdescription) as statusdescription
                ,cast(qt.quantityitems as float) as quantityitems
                ,cast(1 as float) as quantityorder
                ,cast(ot.items as float)/100 as itemsPriceDar 
                ,cast(ot.discounts as float)/100 as DiscountsPrice
                ,cast(ot.shipping as float)/100 as ShippingPrice
                ,cast(ot.tax as float)/100 as tax
                ,cast(o.value as float)/100  as revenue
                ,(cast(o.value as float)/100)-(cast(ot.shipping as float)/100)   as revenue_without_shipping
                ,LOWER(ol.paymentnames) as paymentnames
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

# if __name__ == "__main__":
#     with open("Output.txt", "w") as text_file:
#         text_file.write(vtexsqlscriptsorderslistupdate("6d41d249-d875-41ef-800e-eb0941f6d86f"))
#         print(vtexsqlscriptsorderslistupdate("6d41d249-d875-41ef-800e-eb0941f6d86f"))