
def vtexsqlscriptjson(schema):
    #para colocar um nova query, basta colocar o 'nome do arquivo' :""" query """
    scripts ={ 'faturamento_ecommerce':f""" 
                                       										
									DROP TABLE IF EXISTS tempdata;
                                        create temp table  tempdata  as (
                                        SELECT generate_series as dategenerate FROM generate_series(
                                            '2022-01-01 00:00:00'::timestamp,
                                            (DATE_TRUNC('month', CURRENT_DATE + INTERVAL '7 months') - INTERVAL '1 day')::timestamp,
                                            '1 day'::interval
                                        )
                                        );

                                     
                                        DROP TABLE IF EXISTS faturamentodiario;
                                        create temp table  faturamentodiario  as (

                                        select 
                                        DATE_TRUNC('day',  creationdate)   as dategenerate,
                                        cast(round(cast(SUM(revenue) as numeric),2) as float) as faturamento,
                                        cast(SUM(quantityorder) as float)  as pedidos

                                        from "{schema}".orders_ia ia 
                                        group by 1

                                        );

                  
                                        DROP TABLE IF EXISTS faturamentoprojetado;
                                        create temp table  faturamentoprojetado  as (

                                        select 
                                        DATE_TRUNC('day',  creationdateforecast)   as dateprojecao,
                                        cast(round(cast(SUM(predicted_revenue) as numeric),2) as float)   as faturamento

                                        from "{schema}".orders_ia_forecast ia 
                                        group by 1
										);
                                        

                                        

                                        select 
                                        cast(td.dategenerate as varchar(20)) as datadiaria,
                                        cast(COALESCE(fd.faturamento,0.00)as float) as faturamento,
                                        cast(COALESCE(fd.pedidos,0) as float) as pedidos,
                                        cast(COALESCE(fp.faturamento,0) as float) as faturamentoprojetado,
                                        CASE WHEN to_char(td.dategenerate,'yyyy') = '2024' then cast(round(cast(COALESCE(fd.faturamento,fp.faturamento) as numeric) *0.94 ,2) as float) ELSE 0.00 end as faturamentometa

                                        from tempdata as td

                                        left join faturamentodiario as fd on 
                                        td.dategenerate = fd.dategenerate

                                        left join faturamentoprojetado as fp on 
                                        td.dategenerate = fp.dateprojecao

                                        order by 1 
                                        """ 
                                        
                ,'faturamento_categorias': f"""
                                            
          
                                            SET CLIENT_ENCODING = 'UTF8';
                                                                            
                                            select 
                                            cast(DATE_TRUNC('day',  ori.creationdate) as varchar(20))  as dategenerate,
                                            cast(idcat as integer) as idcategoria,
                                            ori.namecategory as nomecategoria,
                                            cast(idprod as integer) as idsku,
                                            ori.namesku as nomesku ,

                                            cast(SUM(ori.sellingprice) as float)  as faturamento,
                                            cast(SUM(ori.quantityorder) as integer)  as pedidos

                                            from "{schema}".orders_items_ia ori
                                            group by 
                                            1,2,3,4,5 
                                            order by 1    
                                               
                                            """

                 ,'faturamento_compradores': f"""
                                                                                 
                                select 
                                cast(DATE_TRUNC('day',  creationdate) as varchar(20))   as dategenerate,
                                userprofileid 

                                from "{schema}".orders_ia oi 

                                group by 1,2
                                order by 1
                                      """                            
                ,'faturamento_canais': f"""
                                                
                                        SET CLIENT_ENCODING = 'UTF8';
                                        
                                        select 

                                        cast(DATE_TRUNC('day',  ori.creationdate) as varchar(20))  as dategenerate,
                                        'Mercado Livre' as nomecanal,
                                        cast(ori.idprod as integer) as idsku,
                                        ori.namesku as nomesku ,
                                        cast(round(cast(SUM(ori.sellingprice)*1.15 as numeric),2) as float)  as faturamento,
                                        cast(SUM(ori.quantityorder) as integer)  as pedidos

                                        from  "{schema}".orders_ia as ord
                                        inner join  "{schema}".orders_items_ia  as ori on
                                        ord.orderid = ori.orderid

                                        group by 1,3,4

                                        limit 100

                                        """
                                        
                ,'faturamento_regiao': f"""
                                                                        
                                            SET CLIENT_ENCODING = 'UTF8';
                                            
                                            select 
                                            cast(DATE_TRUNC('day',  creationdate) as varchar(20))   as dategenerate,
                                            trim(selectedaddresses_0_state) as estado,
                                            INITCAP(translate(trim(selectedaddresses_0_city),  
                                            'áàâãäåaaaÁÂÃÄÅAAAÀéèêëeeeeeEEEÉEEÈìíîïìiiiÌÍÎÏÌIIIóôõöoooòÒÓÔÕÖOOOùúûüuuuuÙÚÛÜUUUUçÇñÑýÝ',  
                                            'aaaaaaaaaAAAAAAAAAeeeeeeeeeEEEEEEEiiiiiiiiIIIIIIIIooooooooOOOOOOOOuuuuuuuuUUUUUUUUcCnNyY'   
                                            )) as cidade,
                                            cast(SUM(revenue) as float)   as faturamento,
                                            cast(SUM(quantityorder) as integer)  as pedidos

                                            from "{schema}".orders_ia ia 

                                  
                                            

                                            group by 1,2,3
                                            order by 3


                                        """   
                ,'pedido_ecommerce': f""" 
                                                                           
                                      select 
                                      cast(DATE_TRUNC('day',  ori.creationdate) as varchar(20))  as dategenerate,
                                      cast(idcat as integer) as idcategoria,
                                      ori.namecategory as nomecategoria,
                                      cast(idprod as integer) as idsku,
                                      ori.namesku as nomesku ,
                                      trim(selectedaddresses_0_state) as estado,
                                      INITCAP(translate(trim(selectedaddresses_0_city),  
                                      'áàâãäåaaaÁÂÃÄÅAAAÀéèêëeeeeeEEEÉEEÈìíîïìiiiÌÍÎÏÌIIIóôõöoooòÒÓÔÕÖOOOùúûüuuuuÙÚÛÜUUUUçÇñÑýÝ',  
                                      'aaaaaaaaaAAAAAAAAAeeeeeeeeeEEEEEEEiiiiiiiiIIIIIIIIooooooooOOOOOOOOuuuuuuuuUUUUUUUUcCnNyY'   
                                      )) as cidade,
                                      cast(SUM(ori.revenue_without_shipping) as float)  as faturamento,
                                      cast(SUM(ori.quantityorder) as integer)  as pedidos,
                                      cast(SUM(ori.quantityitems) as integer)  as qtditem



                                      from "{schema}".orders_items_ia ori
                                      group by 
                                      1,2,3,4,5,6,7
                                      order by 1 
                                      """                             

                                
    
    }
    # Convertendo o dicionário para uma string JSON
  
    return scripts

# if __name__ == "__main__":
#     with open("Output.txt", "w") as text_file:
#         text_file.write(vtexsqlscriptsorderslistupdate("6d41d249-d875-41ef-800e-eb0941f6d86f"))
#         print(vtexsqlscriptsorderslistupdate("6d41d249-d875-41ef-800e-eb0941f6d86f"))