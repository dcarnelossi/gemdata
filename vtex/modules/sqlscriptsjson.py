
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
                                        WITH faturamento_base_atual AS (
                                                SELECT 
                                                    DATE_TRUNC('day', ori.creationdate) AS dt,
                                                    CAST(idcat AS INTEGER) AS idc,
                                                    concat(cast(idcat AS VARCHAR(10)), '-', ori.namecategory) AS nmc,
                                                    CAST(idprod AS INTEGER) AS ids,
                                                    concat(cast(idprod AS VARCHAR(10)), '-', ori.namesku) AS nms,
                                                    CAST(SUM(ori.revenue_without_shipping) AS FLOAT) AS fat,
                                                    CAST(count(distinct orderid) AS INTEGER) AS ped,
                                                    CAST(sum(ori.quantityitems)  as INTEGER ) as qti
                                                                                           
                                                    from "{schema}".orders_items_ia ori
                                                    
                                                GROUP BY 1, 2, 3, 4, 5
                                            ),
                                            faturamento_passado AS (
  											SELECT 
                                                   -- to_char(DATE_TRUNC('day', ori.creationdate), 'YYYY-MM-DD')- INTERVAL '1 year' AS dt,
  													dt+ INTERVAL '1 year' AS dt,
                                                    idc,
                                                   	nmc,
                                                    ids,
                                                    nms,
                                                    fat as fat_a,
                                                    ped as ped_a,
                                                    qti as qti_a
                                                    
                                                    from faturamento_base_atual
                                            ),    
                                             faturamento_juntos as(
                                            SELECT 
                                            --  base.dateint,
                                                base.dt,
                                                base.idc,
                                                base.nmc,
                                                base.ids,
                                                base.nms,
                                                base.fat,
                                                base.ped,
                                                base.qti,
                                                0 AS fat_a,
                                               0 AS ped_a,
                                               0 as qti_a
                                            FROM faturamento_base_atual base
                                           union all 
												select 
												base.dt,
                                                base.idc,
                                                base.nmc,
                                                base.ids,
                                                base.nms,
                                                0,
                                                0,
                                                0,
                                                base.fat_a AS fat_a,
                                                base.ped_a AS ped_a,
                                                base.qti_a as qti_a
												from faturamento_passado base
												)
												select 
												to_char(base.dt, 'YYYY-MM-DD') as dt,
                                                base.idc,
                                                base.nmc,
                                                base.ids,
                                                base.nms,
                                                CAST(COALESCE(round(cast(sum(base.fat) as decimal),2), 0) as float) as fat,
                                                COALESCE(sum(base.ped), 0) AS ped,
                                                COALESCE(sum(base.qti), 0) AS qti,
                                                CAST(COALESCE(round(cast(sum(base.fat_a) as decimal),2), 0) as float) AS fat_a,
                                                COALESCE(sum(base.ped_a), 0) AS ped_a,
                                                COALESCE(sum(base.qti_a), 0) AS qti_a
                                                
												from faturamento_juntos base
												where base.dt <= (select max( dt) from faturamento_base_atual)
												group by 1,2,3,4,5
												order by 1
                                            	
												
                                            """

                 ,'faturamento_compradores': f"""
                                                                                 
                                select 
                                cast(DATE_TRUNC('day',  creationdate) as varchar(20))   as dategenerate,
                                userprofileid, 
                                cast(round(cast(SUM(revenue) as numeric),2) as float) as faturamento

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
                                        
                ,'faturamento_regiao2': f"""
                                        SET CLIENT_ENCODING = 'UTF8';
                                        WITH faturamento_base_atual AS (
                                           select 
                                            DATE_TRUNC('day', creationdate) AS dt,
                                            coalesce (c2.estado,upper(trim(selectedaddresses_0_state))) as est,
                                            coalesce(c2.cidade,INITCAP(translate(trim(selectedaddresses_0_city),  
                                            'áàâãäåaaaÁÂÃÄÅAAAÀéèêëeeeeeEEEÉEEÈìíîïìiiiÌÍÎÏÌIIIóôõöoooòÒÓÔÕÖOOOùúûüuuuuÙÚÛÜUUUUçÇñÑýÝ',  
                                            'aaaaaaaaaAAAAAAAAAeeeeeeeeeEEEEEEEiiiiiiiiIIIIIIIIooooooooOOOOOOOOuuuuuuuuUUUUUUUUcCnNyY'   
                                            ))) as cid,
                                            concat('BR-', coalesce (c2.estado,upper(trim(selectedaddresses_0_state))) ) as isoest,
                                            cast(SUM(revenue) as float)   as fat,
                                            cast(SUM(quantityorder) as integer)  as ped
											
                                            from "{schema}".orders_ia ia 
                                           left join public.cidades c2 on 
                                            c2.estado = upper(trim(ia.selectedaddresses_0_state))
                                            and 
                                            REPLACE(
													    INITCAP(
													        TRANSLATE(
													            TRIM(selectedaddresses_0_city),
													            'áàâãäåaaaÁÂÃÄÅAAAÀéèêëeeeeeEEEÉEEÈìíîïìiiiÌÍÎÏÌIIIóôõöoooòÒÓÔÕÖOOOùúûüuuuuÙÚÛÜUUUUçÇñÑýÝ',
													            'aaaaaaaaaAAAAAAAAAeeeeeeeeeEEEEEEEiiiiiiiiIIIIIIIIooooooooOOOOOOOOuuuuuuuuUUUUUUUUcCnNyY'
													        )
													    ),
													    ' ',
													    ''
													)  = cidade_bate
											group by 1,2,3,4
                                            order by 1

                                            ),
                                            faturamento_passado AS (
  											SELECT 
                                                   -- to_char(DATE_TRUNC('day', ori.creationdate), 'YYYY-MM-DD')- INTERVAL '1 year' AS dt,
  													dt+ INTERVAL '1 year' AS dt,
                                                   	est,
                                                   	cid, 
                                                   	isoest,
                                                   	fat as fat_a,
                                                   	ped as ped_a
                                                    
                                                    from faturamento_base_atual
                                            ),    
                                             faturamento_juntos as(
                                            SELECT 
                                            --  base.dateint,
                                                base.dt,
                                                base.est,
                                                base.cid, 
                                                isoest,
                                                base.fat as fat,
                                                base.ped as ped,
                                                0 as fat_a,
                                                0 as ped_a
                                            FROM faturamento_base_atual base
                                           union all 
												select 
												base.dt,
                                                base.est,
                                                base.cid, 
                                                isoest,
                                                0 as fat,
                                                0 as ped,
                                                base.fat_a, 
                                                base.ped_a
                                                from faturamento_passado base
												)
												select 
												to_char(base.dt, 'YYYY-MM-DD') as dt,
                                                base.est,
                                                base.cid, 
                                                base.isoest,
                                                CAST(COALESCE(round(cast(sum(base.fat) as decimal),2), 0) as float) as fat,
                                                COALESCE(sum(base.ped), 0) AS ped,
                                                CAST(COALESCE(round(cast(sum(base.fat_a) as decimal),2), 0) as float) AS fat_a,
                                                COALESCE(sum(base.ped_a), 0) AS ped_a
                                                
												from faturamento_juntos base
												where base.dt <= (select max( dt) from faturamento_base_atual)
												group by 1,2,3,4
												order by 1

                                        """   
                ,'pedido_ecommerce': f""" 
                            WITH faturamento_base AS (
                                SELECT 
                                    to_char(DATE_TRUNC('day', ori.creationdate), 'YYYY-MM-DD') AS dt,
                                    CAST(idcat AS INTEGER) AS idc,
                                    concat(cast(idcat AS VARCHAR(10)), '-', ori.namecategory) AS nmc,
                                    CAST(idprod AS INTEGER) AS ids,
                                    concat(cast(idprod AS VARCHAR(10)), '-', ori.namesku) AS nms,
                                    trim(selectedaddresses_0_state) as est,
                                    INITCAP(translate(trim(selectedaddresses_0_city),  
                                                            'áàâãäåaaaÁÂÃÄÅAAAÀéèêëeeeeeEEEÉEEÈìíîïìiiiÌÍÎÏÌIIIóôõöoooòÒÓÔÕÖOOOùúûüuuuuÙÚÛÜUUUUçÇñÑýÝ',  
                                                            'aaaaaaaaaAAAAAAAAAeeeeeeeeeEEEEEEEiiiiiiiiIIIIIIIIooooooooOOOOOOOOuuuuuuuuUUUUUUUUcCnNyY'   
                                                            )) as cid,
                                    cast(SUM(ori.revenue_without_shipping) as float)  as fat,
                                    cast(SUM(ori.quantityorder) as integer)  as ped,
                                    cast(SUM(ori.quantityitems) as integer)  as qti

                            
                                    from "{schema}".orders_items_ia ori
                                GROUP BY 1, 2, 3, 4, 5,6,7
                            )
                            SELECT 
                            --  base.dateint,
                                base.dt,
                                base.idc,
                                base.nmc,
                                base.ids,
                                base.nms,
                                base.est,
                                base.cid,
                                base.fat,
                                base.ped,
                                base.qti,
                                CAST(COALESCE(round(cast(prev.fat as decimal),2), 0) as float) AS fat_a,
                                COALESCE(prev.ped, 0) AS ped_a,
                                COALESCE(prev.qti, 0) AS qti_a

                            FROM faturamento_base base
                            LEFT JOIN faturamento_base prev
                                ON base.idc = prev.idc
                                AND base.ids = prev.ids
                                and base.est = base.est
                                and base.cid = base.cid
                                AND base.dt = TO_CHAR((TO_DATE(prev.dt, 'YYYY-MM-DD') + INTERVAL '1 year'), 'YYYY-MM-DD')
                            ORDER BY base.dt;
                                      """                             
                ,'faturamento_mensal': f""" 
                                                            
                                        select 
                                        yearMonth,
                                        "date",
                                        cast(round(cast(sum(revenue) as numeric),0) as varchar(20)) as revenue,
                                        cast(round(cast(sum(projection) as numeric),0) as varchar(20)) as projection,
                                        cast(round(cast(sum(orders) as numeric),0) as varchar(20)) as orders,
                                        cast(round(cast(sum(averageTicket) as numeric),0) as varchar(20)) as averageTicket 
                                        from (
                                            select 
                                            to_char(creationdate,'YYYY-MM') as yearMonth,
                                            to_char(DATE_TRUNC('month', creationdate),'YYYY-MM-DD' )as "date",
                                            sum(oi.revenue) as revenue,
                                            0 as projection,
                                            count(1) as orders, 
                                            sum(oi.revenue) / count(1) as averageTicket
                                            from "{schema}".orders_ia oi 
                                            group by 
                                            1,2
                                            
                                            union all 
                                            
                                            select 
                                            to_char(creationdateforecast,'YYYY-MM') as yearMonth,
                                            to_char(DATE_TRUNC('month', creationdateforecast),'YYYY-MM-DD' ) as "date",
                                            0 as revenue,
                                            SUM(predicted_revenue) as projection,
                                            0 as orders,
                                            0 as averageTicket
                                            from "{schema}".orders_ia_forecast oif   
                                            group by 1,2
                                        )mesorder
                                        group by 1,2 
                                        order by  1 
                                      """                             
                ,'pedido_por_categoria': f"""
                                     
										SET CLIENT_ENCODING = 'UTF8';
                                        WITH faturamento_base_atual AS (
                                               select
                                             		DATE_TRUNC('day', ori.creationdate) AS dt,
                                                    concat(cast(idcat AS VARCHAR(10)), '-', ori.namecategory) AS nmc,
                                                    CAST(count(distinct orderid) AS INTEGER) AS ped_cat
                                                                                           
                                               from "{schema}".orders_items_ia ori
                                                GROUP BY 1,2	
                                            ),
                                            faturamento_passado AS (
  											SELECT 
                                                   -- to_char(DATE_TRUNC('day', ori.creationdate), 'YYYY-MM-DD')- INTERVAL '1 year' AS dt,
  													dt+ INTERVAL '1 year' AS dt,
                                                   	nmc,
                                                   	ped_cat as ped_cat_a
                                                    
                                                    from faturamento_base_atual
                                            ),    
                                             faturamento_juntos as(
                                            SELECT 
                                            --  base.dateint,
                                                base.dt,
                                                base.nmc,
                                                base.ped_cat,
                                                0 as ped_cat_a
                                                
                                            FROM faturamento_base_atual base
                                           union all 
												select 
												base.dt,
                                                base.nmc,
                                                0,
                                                base.ped_cat_a as ped_cat_a
												from faturamento_passado base
												)
												select 
												to_char(base.dt, 'YYYY-MM-DD') as dt,
                                                base.nmc,
                                                COALESCE(sum(base.ped_cat), 0) AS ped_cat,
                                                COALESCE(sum(base.ped_cat_a), 0) AS ped_cat_a
                                                
												from faturamento_juntos base
												where base.dt <= (select max( dt) from faturamento_base_atual)
												group by 1,2
												order by 1
                                            	
													
                    """
    }
    # Convertendo o dicionário para uma string JSON
  
    return scripts

# if __name__ == "__main__":
#     with open("Output.txt", "w") as text_file:
#         text_file.write(vtexsqlscriptsorderslistupdate("6d41d249-d875-41ef-800e-eb0941f6d86f"))
#         print(vtexsqlscriptsorderslistupdate("6d41d249-d875-41ef-800e-eb0941f6d86f"))