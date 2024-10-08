def vtexsqlscriptscreatetabglobaldemo(schema):
    scripts = f"""
                                
            CREATE SCHEMA IF NOT EXISTS demonstracao;


            DROP TABLE IF EXISTS "demonstracao".orders_items_ia;
            create TABLE "demonstracao".orders_items_ia
            as
            SELECT 
            orderid,
            creationdate,
            idcat,
            namecategory,
            idprod,   
            namesku,
            sellingprice * (1.05 + (RANDOM() * (0.20))) as sellingprice ,
            quantityorder * (2 + (RANDOM() * (0.20))) as quantityorder,
            quantityitems * (2 + (RANDOM() * (0.20))) as quantityitems,
            revenue_without_shipping * (1.05 + (RANDOM() * (0.20))) as revenue_without_shipping,
            selectedaddresses_0_city,
            selectedaddresses_0_state
            FROM "{schema}".orders_items_ia  ;
            

            DROP TABLE IF EXISTS "demonstracao".orders_ia;
            create TABLE "demonstracao".orders_ia
            as
            SELECT 
            orderid,
            creationdate,
            userprofileid,
            revenue * (1.05 + (RANDOM() * (0.20))) as revenue,
            quantityorder * (2 + (RANDOM() * (0.20))) as quantityorder,
            selectedaddresses_0_state,
            selectedaddresses_0_city

            FROM "{schema}".orders_ia  ;
                

    """
    print(scripts)
    return scripts
