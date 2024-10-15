def vtexsqlscriptscreatetabglobaldemo(schemacopy,schema):
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
                        FROM '{schemacopy}'.orders_items_ia  ;
                        

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

                        FROM '{schemacopy}'.orders_ia  ;
                        
                        

                

    """
    # print(scripts)
    return scripts
