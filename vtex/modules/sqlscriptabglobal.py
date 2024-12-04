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

    """
    # print(scripts)
    return scripts


def shopifysqlscriptscreatetabglobal(schema):
    scripts = f"""
                INSERT INTO "{schema}".shopify_gemdata_categoria(nomecategoria)
                select distinct  
                (LOWER( case WHEN TRIM(COALESCE(si.producttype, '')) = '' THEN 'não informado' ELSE si.producttype END)) as nomecategoria
                from "{schema}".shopify_orders_items si 
                where 
                (LOWER( case WHEN TRIM(COALESCE(si.producttype, '')) = '' THEN 'não informado' ELSE si.producttype END))
                not in  
                (select oi.nomecategoria from  "{schema}".shopify_gemdata_categoria oi);

            	DROP TABLE IF EXISTS orderspayment;
                CREATE TEMPORARY TABLE orderspayment as	
                SELECT DISTINCT ON (orderid) orderid, gateway
				FROM "{schema}".shopify_orders_payment
				ORDER BY orderid, amount DESC;
				
				DROP TABLE IF EXISTS ordersfretegratis;
                CREATE TEMPORARY TABLE ordersfretegratis as
                select distinct  so.orderid,case when cast(so.totalshippingprice as numeric) =0 then  'true' else  'false' end  isFreeShipping  
                from  "{schema}".shopify_orders so  ;  

                DROP TABLE IF EXISTS "{schema}".orders_items_ia;
                create TABLE "{schema}".orders_items_ia
                as
                select 
                date_trunc('hour',so.createdat AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' ) as creationdate ,
                so.orderid,
                --tem mas é diferente do vtex
                coalesce( substring(si.title FROM '- ([0-9]+)$'),'999999') as idprod ,
                LOWER(coalesce(si.title,'Não informado')) as namesku,
                --não tem no shopify
                coalesce(ca.idcategoriagemdata,'999999') as idcat,
                LOWER( case WHEN TRIM(COALESCE(si.producttype, '')) = '' THEN 'não informado' ELSE si.producttype END) as namecategory,
                0 as tax,
                0 as taxcode,
                cast(1 as float) as quantityorder,
                cast(si.quantity as float) as quantityitems ,
                cast(si.originalunitprice as float) as price,
                cast(si.originalunitprice as float)  as costprice,
                cast(si.originalunitprice as float) as listprice,
                cast(0.00 as float)/100 as commission,
                --cast(oi.shippingprice as numeric)/100 as shippingprice,
                cast(cast(si.originalunitprice as float) - cast(si.totaldiscountamount as float) as float) as sellingprice,
                cast(si.totaldiscountamount as float)  as totaldiscounts,
                cast((cast(cast(si.originalunitprice as float)*cast(si.quantity as float) as float)) - cast(si.totaldiscountamount as float) as float)  as revenue_without_shipping ,
                false as isgift,
                LOWER(coalesce(so.shippingcity,so.billingcity)) as selectedaddresses_0_city,
                LOWER(coalesce(so.shippingprovincecode,so.billingprovincecode)) as selectedaddresses_0_state,
                LOWER(coalesce(so.shippingcountrycode,so.billingcountrycode)) as selectedaddresses_0_country,
                so.email as userprofileid,
                coalesce(LOWER(op.gateway),'nao informado') as paymentnames,
                --,oi.saleschannel as saleschannel
                LOWER(so.displayfinancialstatus) as statusdescription,
                LOWER(coalesce(so.channelname,'nao informado')) as origin,
                fg.isFreeShipping,
                (cast(so.totalprice as float))-(cast(so.totalshippingprice as float)) as revenue_orders_out_ship


                from "{schema}".shopify_orders_items si 

                inner join "{schema}".shopify_orders so  on 
                so.orderid = si.orderid
				
                left join "{schema}".shopify_gemdata_categoria ca on 
                ca.nomecategoria = 
                LOWER( case WHEN TRIM(COALESCE(si.producttype, '')) = '' THEN 'não informado' ELSE si.producttype END)

                left join orderspayment op on 
                op.orderid = so.orderid
               

                left join ordersfretegratis fg 
                on fg.orderid = si.orderid

     
               where 
                LOWER(so.displayfinancialstatus)  in  ('paid') and so.cancelledat is null;


               --COMECANDO AQUI 
               --------------------------------------------------------------------------------
                DROP TABLE IF EXISTS qtditemorder;
                CREATE TEMPORARY TABLE qtditemorder as
                select oi.orderid, sum(cast(oi.quantity as numeric)) as quantityitems  
                from "{schema}".shopify_orders_items  oi 
                group by orderid;


                DROP TABLE IF EXISTS "{schema}".orders_ia;

                create table "{schema}".orders_ia
                as
                select 
                date_trunc('hour',o.createdat AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' ) as creationdate  
                ,o.orderid as orderid
                ,LOWER('NAO INFORMADO') as origin
                ,LOWER(coalesce(o.channelname,'nao informado')) as saleschannel
                ,LOWER(o.displayfinancialstatus) as statusdescription
                ,cast(qt.quantityitems as float) as quantityitems
                ,cast(1 as float) as quantityorder
                --,cast(o.items as float) as itemsPriceDar 
                ,cast(o.currenttotaldiscounts as float) as DiscountsPrice
                ,cast(o.totalshippingprice as float) as ShippingPrice
                ,cast(o.totaltax as float) as tax
                ,cast(o.totalprice as float)  as revenue
                ,(cast(o.totalprice as float))-(cast(o.totalshippingprice as float))   as revenue_without_shipping
                --IMPORTANTE 
                ,coalesce(LOWER(op.gateway),'nao informado') as paymentnames
                ,LOWER(coalesce(o.shippingcity,o.billingcity)) as selectedaddresses_0_city
                ,LOWER(coalesce(o.shippingprovincecode,o.billingprovincecode)) as selectedaddresses_0_state
                ,LOWER(coalesce(o.shippingcountrycode,o.billingcountrycode)) as selectedaddresses_0_country
                ,o.email as userprofileid
                ,case when cast(o.totalshippingprice as numeric) =0 then  'Sem Frete' else  'Com Frete' end  FreeShipping 
                ,case when cast(o.totalshippingprice as numeric) =0 then  'true' else  'false' end  isFreeShipping 
                ,case when round( cast(random() * (1 - 0) as numeric),0)=0 then 'F' else 'M' end   as Sexo 


                from "{schema}".shopify_orders o
                
                left join orderspayment op on 
                op.orderid = o.orderid
               
                left join qtditemorder qt on 
                qt.orderid = o.orderid

                where 
                LOWER(o.displayfinancialstatus)  in  ('paid') and o.cancelledat is null;
				
                                

    """
    # print(scripts)
    return scripts

# if __name__ == "__main__":
#     with open("Output.txt", "w") as text_file:
#         text_file.write(vtexsqlscriptsorderslistupdate("6d41d249-d875-41ef-800e-eb0941f6d86f"))
#         print(vtexsqlscriptsorderslistupdate("6d41d249-d875-41ef-800e-eb0941f6d86f"))