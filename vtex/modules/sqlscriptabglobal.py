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
                date_trunc('hour',ol.creationdate) as creationdate ,
                oi.orderid,
                coalesce(oi.sellersku,'999999') as idprod ,
                coalesce(sku.namecomplete,'Não informado') as namesku,
                coalesce(pro.categoryid,'999999') as idcat,
                coalesce(cat.name,'Não informado') as namecategory,
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
                ,sd.selectedaddresses_0_city
                ,sd.selectedaddresses_0_state
                ,sd.selectedaddresses_0_country
                ,cp.userprofileid
                ,ol.paymentnames
                --,oi.saleschannel as saleschannel
                ,ol.statusdescription
                ,ol.origin
                ,fg.isFreeShipping
                ,value as revenue_orders


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
                        
                where 
                ol.statusdescription  = 'Faturado';


                DROP TABLE IF EXISTS qtditemorder;
                CREATE TEMPORARY TABLE qtditemorder as
                select oi.orderid, sum(cast(oi.quantity as numeric)) as quantityitems  
                from "{schema}".orders_items oi 
                group by orderid;



                DROP TABLE IF EXISTS "{schema}".orders_ia;

                create table "{schema}".orders_ia
                as
                select 
                date_trunc('hour',ol.creationdate) as creationdate 
                ,o.orderid
                ,o.origin
                ,O.saleschannel
                ,o.statusdescription
                ,cast(qt.quantityitems as float) as quantityitems
                ,cast(1 as float) as quantityorder
                ,cast(ot.items as float)/100 as itemsPriceDar 
                ,cast(ot.discounts as float)/100 as DiscountsPrice
                ,cast(ot.shipping as float)/100 as ShippingPrice
                ,cast(ot.tax as float)/100 as tax
                ,cast(o.value as float)/100  as revenue
                ,(cast(o.value as float)/100)-(cast(ot.shipping as float)/100)   as revenue_without_shipping
                ,ol.paymentnames
                ,sd.selectedaddresses_0_city
                ,sd.selectedaddresses_0_state
                ,sd.selectedaddresses_0_country
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
                ol.statusdescription  = 'Faturado';

    """
    print(scripts)
    return scripts

# if __name__ == "__main__":
#     with open("Output.txt", "w") as text_file:
#         text_file.write(vtexsqlscriptsorderslistupdate("6d41d249-d875-41ef-800e-eb0941f6d86f"))
#         print(vtexsqlscriptsorderslistupdate("6d41d249-d875-41ef-800e-eb0941f6d86f"))