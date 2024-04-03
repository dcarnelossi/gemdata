
import os
import psycopg2
from psycopg2 import sql
from datetime import datetime
import psycopg2.extensions
from vtex.modules.dbpgconn import *

api_conection_info=None
data_conection_info=None
coorp_conection_info=None

    

#inserir_query_estrutura = aqui é onde tem o comando para inserir na tabela corporativo.criar_estrutura 
def write_query_structure(id_integration ,start_create_date,type_structure_postgree,code_sql,sequence_execution,end_status_execution,activate_json_execution,name_json_file):
    try:
        
        #query = "INSERT INTO integration_create_structure (id_integration,start_create_date,type_structure_postgree,code_sql,sequence_execution,end_status_execution,activate_json_execution,name_json_file)  VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
        query = { 'id_integration': id_integration, 'start_create_date' : start_create_date ,'type_structure_postgree': type_structure_postgree,'code_sql': code_sql,'sequence_execution' : sequence_execution,'end_status_execution' : end_status_execution,'activate_json_execution' : activate_json_execution,'name_json_file' : name_json_file }  
        
        writer = WriteJsonToPostgres(coorp_conection_info, query, "integration_create_structure")
        retorno= writer.insert_data()
        # Executa a consulta
        #cur.execute(query,(hash_cadastro_api,data_criacao,tipo_pg,codigo_sql,sequencia_execucao,status_executado,gerar_json,nome_sql_json))
        #conexao.commit()
        return retorno
    
    except Exception as e:
        return e



def white_integration_create_structure():
    try:
          
        write_query_structure(coorp_id_create_structure,datetime.now(),'S','CREATE SCHEMA IF NOT EXISTS "'+ coorp_id_create_structure + '"','1','0','0','schema') 
        #query para tabela brands
        write_query_structure(coorp_id_create_structure,datetime.now(),'T',f"""
                                    CREATE TABLE IF NOT EXISTS """+ '"'  + coorp_id_create_structure+ '"'  + """.brands
                                        (
                                        sequence SERIAL PRIMARY KEY,
                                        id integer,
                                        name text COLLATE pg_catalog."default",
                                        text text COLLATE pg_catalog."default",
                                        keywords text COLLATE pg_catalog."default",
                                        sitetitle text COLLATE pg_catalog."default",
                                        active boolean,
                                        menuhome boolean,
                                        adwordsremarketingcode text COLLATE pg_catalog."default",
                                        lomadeecampaigncode text COLLATE pg_catalog."default",
                                        score double precision,
                                        linkid text COLLATE pg_catalog."default",
                                        data_insercao timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
                                        CONSTRAINT constraint_brands_id UNIQUE (id)
                                        ) TABLESPACE pg_default; 
                                        ALTER TABLE IF EXISTS """ + '"'  + coorp_id_create_structure+ '"'  + """.brands
                                        OWNER to adminuserpggemdatadev;
                                        """ ,'2','0','0','brands') 
             #query para tabela categories
        write_query_structure(coorp_id_create_structure,datetime.now(),'T',f"""
                                    CREATE TABLE IF NOT EXISTS """+ '"'  + coorp_id_create_structure+ '"'  + """.categories
                                       (
                                        sequence SERIAL PRIMARY KEY,
                                        id integer,
                                        name text COLLATE pg_catalog."default",
                                        fathercategoryid integer,
                                        title text COLLATE pg_catalog."default",
                                        description text COLLATE pg_catalog."default",
                                        keywords text COLLATE pg_catalog."default",
                                        isactive boolean,
                                        treepath text COLLATE pg_catalog."default",
                                        lomadeecampaigncode text COLLATE pg_catalog."default",
                                        adwordsremarketingcode text COLLATE pg_catalog."default",
                                        showinstorefront boolean,
                                        showbrandfilter boolean,
                                        activestorefrontlink boolean,
                                        globalcategoryid integer,
                                        stockkeepingunitselectionmode text COLLATE pg_catalog."default",
                                        score numeric,
                                        linkid text COLLATE pg_catalog."default",
                                        haschildren boolean,
                                        data_insercao timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
                                        CONSTRAINT constraint_categories_id UNIQUE (id)
                                        ) TABLESPACE pg_default; 
                                        ALTER TABLE IF EXISTS """ + '"'  + coorp_id_create_structure+ '"'  + """.categories
                                        OWNER to adminuserpggemdatadev;
                                        """ ,'3','0','0','categories') 
              #query para tabela client_profile
        write_query_structure(coorp_id_create_structure,datetime.now(),'T',f"""
                                    CREATE TABLE IF NOT EXISTS """+ '"'  + coorp_id_create_structure+ '"'  + """.client_profile
                                        (
                                        sequence SERIAL PRIMARY KEY,
                                        orderid text COLLATE pg_catalog."default",
                                        id text COLLATE pg_catalog."default",
                                        email text COLLATE pg_catalog."default",
                                        phone text COLLATE pg_catalog."default",
                                        document text COLLATE pg_catalog."default",
                                        lastname text COLLATE pg_catalog."default",
                                        firstname text COLLATE pg_catalog."default",
                                        tradename text COLLATE pg_catalog."default",
                                        iscorporate boolean,
                                        documenttype text COLLATE pg_catalog."default",
                                        corporatename text COLLATE pg_catalog."default",
                                        customerclass text COLLATE pg_catalog."default",
                                        customercode text COLLATE pg_catalog."default",
                                        userprofileid uuid,
                                        corporatephone text COLLATE pg_catalog."default",
                                        stateinscription text COLLATE pg_catalog."default",
                                        corporatedocument text COLLATE pg_catalog."default",
                                        userprofileversion text COLLATE pg_catalog."default",
                                        data_insercao timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
                                        CONSTRAINT constraint_client_profile_orderid UNIQUE (orderid)
                                        ) TABLESPACE pg_default; 
                                        CREATE INDEX idx_client_profile_orderid ON """ + '"'  + coorp_id_create_structure+ '"'  + """.client_profile USING btree (orderid);
                                        ALTER TABLE IF EXISTS """ + '"'  + coorp_id_create_structure+ '"'  + """.client_profile
                                        OWNER to adminuserpggemdatadev;
                                        """ ,'4','0','0','client_profile')             
             #query para tabela orders
        write_query_structure(coorp_id_create_structure,datetime.now(),'T',f"""
                                    CREATE TABLE IF NOT EXISTS """+ '"'  + coorp_id_create_structure+ '"'  + """.orders
                                        (
                                        sequence SERIAL PRIMARY KEY,
                                        orderid text COLLATE pg_catalog."default",
                                        marketplaceorderid text COLLATE pg_catalog."default",
                                        marketplaceservicesendpoint text COLLATE pg_catalog."default",
                                        sellerorderid text COLLATE pg_catalog."default",
                                        origin text COLLATE pg_catalog."default",
                                        affiliateid text COLLATE pg_catalog."default",
                                        saleschannel text COLLATE pg_catalog."default",
                                        merchantname text COLLATE pg_catalog."default",
                                        status text COLLATE pg_catalog."default",
                                        workflowisinerror boolean,
                                        statusdescription text COLLATE pg_catalog."default",
                                        value integer,
                                        creationdate timestamp without time zone,
                                        lastchange timestamp without time zone,
                                        ordergroup text COLLATE pg_catalog."default",
                                        totals jsonb,
                                        items jsonb,
                                        marketplaceitems jsonb,
                                        clientprofiledata jsonb,
                                        giftregistrydata jsonb,
                                        marketingdata jsonb,
                                        ratesandbenefitsdata jsonb,
                                        shippingdata jsonb,
                                        paymentdata jsonb,
                                        packageattachment jsonb,
                                        sellers jsonb,
                                        callcenteroperatordata jsonb,
                                        followupemail text COLLATE pg_catalog."default",
                                        lastmessage jsonb,
                                        hostname text COLLATE pg_catalog."default",
                                        invoicedata jsonb,
                                        changesattachment jsonb,
                                        opentextfield jsonb,
                                        roundingerror numeric,
                                        orderformid text COLLATE pg_catalog."default",
                                        commercialconditiondata jsonb,
                                        iscompleted boolean,
                                        customdata jsonb,
                                        storepreferencesdata jsonb,
                                        allowcancellation boolean,
                                        allowedition boolean,
                                        ischeckedin boolean,
                                        marketplace jsonb,
                                        authorizeddate timestamp without time zone,
                                        invoiceddate timestamp without time zone,
                                        cancelreason text COLLATE pg_catalog."default",
                                        itemmetadata jsonb,
                                        subscriptiondata jsonb,
                                        taxdata jsonb,
                                        checkedinpickuppointid text COLLATE pg_catalog."default",
                                        cancellationdata jsonb,
                                        clientpreferencesdata jsonb,
                                        data_insercao timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
                                        CONSTRAINT constraint_orders_orderid UNIQUE (orderid)
                                        ) TABLESPACE pg_default; 
                                        ALTER TABLE IF EXISTS """ + '"'  + coorp_id_create_structure+ '"'  + """.orders
                                        OWNER to adminuserpggemdatadev;
                                        CREATE INDEX IF NOT EXISTS "idx_orders_orderid"
                                                ON """ + '"'  + coorp_id_create_structure+ '"'  + """.orders USING btree
                                                (orderid COLLATE pg_catalog."default" ASC NULLS LAST)
                                                TABLESPACE pg_default;
                                        """ ,'5','0','0','orders') 
                          #query para tabela orders_items
        write_query_structure(coorp_id_create_structure,datetime.now(),'T',f"""
                                    CREATE TABLE IF NOT EXISTS """+ '"'  + coorp_id_create_structure+ '"'  + """.orders_items
                                        (
                                        sequence SERIAL PRIMARY KEY,
                                        id integer,
                                        orderid text COLLATE pg_catalog."default",
                                        ean text COLLATE pg_catalog."default",
                                        tax numeric(10,2),
                                        name text COLLATE pg_catalog."default",
                                        price integer,
                                        refid text COLLATE pg_catalog."default",
                                        isgift boolean,
                                        lockid text COLLATE pg_catalog."default",
                                        params jsonb,
                                        seller text COLLATE pg_catalog."default",
                                        taxcode text COLLATE pg_catalog."default",
                                        imageurl text COLLATE pg_catalog."default",
                                        quantity integer,
                                        uniqueid text COLLATE pg_catalog."default",
                                        costprice integer,
                                        detailurl text COLLATE pg_catalog."default",
                                        listprice integer,
                                        offerings jsonb,
                                        pricetags jsonb,
                                        productid text COLLATE pg_catalog."default",
                                        sellersku text COLLATE pg_catalog."default",
                                        assemblies jsonb,
                                        commission numeric(10,2),
                                        components jsonb,
                                        attachments jsonb,
                                        bundleitems jsonb,
                                        manualprice numeric(10,2),
                                        presaledate date,
                                        rewardvalue numeric(10,2),
                                        sellingprice numeric(10,2),
                                        serialnumbers jsonb,
                                        shippingprice numeric(10,2),
                                        additionalinfo jsonb,
                                        itemattachment jsonb,
                                        unitmultiplier numeric(10,2),
                                        measurementunit text COLLATE pg_catalog."default",
                                        parentitemindex integer,
                                        pricedefinition jsonb,
                                        pricevaliduntil date,
                                        freightcommission numeric(10,2),
                                        callcenteroperator text COLLATE pg_catalog."default",
                                        attachmentofferings jsonb,
                                        parentassemblybinding jsonb,
                                        data_insercao timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
                                        CONSTRAINT constraint_orders_items_orderid UNIQUE (uniqueid)
                                        ) TABLESPACE pg_default; 
                                        CREATE INDEX idx_orders_items_orderid ON """ + '"'  + coorp_id_create_structure+ '"'  + """.orders_items USING btree (orderid);
                                        ALTER TABLE IF EXISTS """ + '"'  + coorp_id_create_structure+ '"'  + """.orders_items
                                        OWNER to adminuserpggemdatadev;
                                        """ ,'6','0','0','orders_items') 
                          #query para tabela orders_list
        write_query_structure(coorp_id_create_structure,datetime.now(),'T',f"""
                                    CREATE TABLE IF NOT EXISTS """+ '"'  + coorp_id_create_structure+ '"'  + """.orders_list
                                        (
                                        sequence_id SERIAL PRIMARY KEY,
                                        orderid text COLLATE pg_catalog."default",
                                        creationdate timestamp without time zone,
                                        clientname text COLLATE pg_catalog."default",
                                        items jsonb,
                                        totalvalue numeric(10,2),
                                        paymentnames text COLLATE pg_catalog."default",
                                        status text COLLATE pg_catalog."default",
                                        statusdescription text COLLATE pg_catalog."default",
                                        marketplaceorderid text COLLATE pg_catalog."default",
                                        sequence text COLLATE pg_catalog."default",
                                        saleschannel text COLLATE pg_catalog."default",
                                        affiliateid text COLLATE pg_catalog."default",
                                        origin text COLLATE pg_catalog."default",
                                        workflowinerrorstate boolean,
                                        workflowinretry boolean,
                                        lastmessageunread text COLLATE pg_catalog."default",
                                        shippingestimateddate timestamp without time zone,
                                        shippingestimateddatemax timestamp without time zone,
                                        shippingestimateddatemin timestamp without time zone,
                                        orderiscomplete boolean,
                                        listid text COLLATE pg_catalog."default",
                                        listtype text COLLATE pg_catalog."default",
                                        authorizeddate timestamp without time zone,
                                        callcenteroperatorname text COLLATE pg_catalog."default",
                                        totalitems integer,
                                        currencycode text COLLATE pg_catalog."default",
                                        hostname text COLLATE pg_catalog."default",
                                        invoiceoutput jsonb,
                                        invoiceinput jsonb,
                                        lastchange timestamp without time zone,
                                        isalldelivered boolean,
                                        isanydelivered boolean,
                                        giftcardproviders jsonb,
                                        orderformid text COLLATE pg_catalog."default",
                                        paymentapproveddate timestamp without time zone,
                                        readyforhandlingdate timestamp without time zone,
                                        deliverydates jsonb,
                                        data_insercao timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
                                        CONSTRAINT constraint_orders_list_orderid UNIQUE (orderid)
                                        ) TABLESPACE pg_default;
                                         CREATE INDEX idx_orders_list_orderid ON """ + '"'  + coorp_id_create_structure+ '"'  + """.orders_list USING btree (orderid);
                                        ALTER TABLE IF EXISTS """ + '"'  + coorp_id_create_structure+ '"'  + """.orders_list
                                        OWNER to adminuserpggemdatadev;
                                        """ ,'7','0','0','orders_list') 
            
              #query para tabela orders_shippingdata
        write_query_structure(coorp_id_create_structure,datetime.now(),'T',f"""
                                    CREATE TABLE IF NOT EXISTS """+ '"'  + coorp_id_create_structure+ '"'  + """.orders_shippingdata
                                        (
                                        sequence SERIAL PRIMARY KEY,
                                        id text COLLATE pg_catalog."default",
                                        orderid text COLLATE pg_catalog."default",
                                        selectedaddresses_0_city text COLLATE pg_catalog."default",
                                        selectedaddresses_0_state text COLLATE pg_catalog."default",
                                        selectedaddresses_0_number text COLLATE pg_catalog."default",
                                        selectedaddresses_0_street text COLLATE pg_catalog."default",
                                        selectedaddresses_0_country text COLLATE pg_catalog."default",
                                        selectedaddresses_0_entityid text COLLATE pg_catalog."default",
                                        selectedaddresses_0_addressid text COLLATE pg_catalog."default",
                                        selectedaddresses_0_reference text COLLATE pg_catalog."default",
                                        selectedaddresses_0_versionid text COLLATE pg_catalog."default",
                                        selectedaddresses_0_complement text COLLATE pg_catalog."default",
                                        selectedaddresses_0_postalcode text COLLATE pg_catalog."default",
                                        selectedaddresses_0_addresstype text COLLATE pg_catalog."default",
                                        selectedaddresses_0_neighborhood text COLLATE pg_catalog."default",
                                        selectedaddresses_0_receivername text COLLATE pg_catalog."default",
                                        selectedaddresses_0_geocoordinates_0 double precision,
                                        selectedaddresses_0_geocoordinates_1 double precision,
                                        address_city text COLLATE pg_catalog."default",
                                        address_state text COLLATE pg_catalog."default",
                                        address_number text COLLATE pg_catalog."default",
                                        address_street text COLLATE pg_catalog."default",
                                        address_country text COLLATE pg_catalog."default",
                                        address_entityid text COLLATE pg_catalog."default",
                                        address_addressid text COLLATE pg_catalog."default",
                                        address_reference text COLLATE pg_catalog."default",
                                        address_versionid text COLLATE pg_catalog."default",
                                        address_complement text COLLATE pg_catalog."default",
                                        address_postalcode text COLLATE pg_catalog."default",
                                        address_addresstype text COLLATE pg_catalog."default",
                                        address_neighborhood text COLLATE pg_catalog."default",
                                        address_receivername text COLLATE pg_catalog."default",
                                        address_geocoordinates_0 double precision,
                                        address_geocoordinates_1 double precision,
                                        data_insercao timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
                                        CONSTRAINT constraint_orders_shippingdata_orderid UNIQUE (orderid)
                                        ) TABLESPACE pg_default; 
                                        ALTER TABLE IF EXISTS """ + '"'  + coorp_id_create_structure+ '"'  + """.orders_shippingdata
                                        OWNER to adminuserpggemdatadev;
                                        CREATE INDEX IF NOT EXISTS idx_orders_shippingdata
                                        ON """ + '"'  + coorp_id_create_structure+ '"'  + """.orders_shippingdata USING btree
                                        (orderid COLLATE pg_catalog."default" ASC NULLS LAST)
                                        TABLESPACE pg_default;
                                        """ ,'8','0','0','orders_shippingdata')           
                          #query para tabela orders_totals
        write_query_structure(coorp_id_create_structure,datetime.now(),'T',f"""
                                    CREATE TABLE IF NOT EXISTS """+ '"'  + coorp_id_create_structure+ '"'  + """.orders_totals
                                        (
                                        sequence SERIAL PRIMARY KEY,
                                        orderid text COLLATE pg_catalog."default",
                                        change integer,
                                        shipping integer,
                                        items integer,
                                        tax integer,
                                        discounts integer,
                                        alternativeshippingtotal integer,
                                        alternativeshippingdiscount integer,
                                        data_insercao timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
                                        CONSTRAINT constraint_orders_totals_orderid UNIQUE (orderid)
                                        ) TABLESPACE pg_default; 
                                        CREATE INDEX idx_orders_totals ON """ + '"'  + coorp_id_create_structure+ '"'  + """.orders_totals USING btree (orderid);
                                        ALTER TABLE IF EXISTS """ + '"'  + coorp_id_create_structure+ '"'  + """.orders_totals
                                        OWNER to adminuserpggemdatadev;
                                        """ ,'9','0','0','orders_totals')   
            
                          #query para tabela products
        write_query_structure(coorp_id_create_structure,datetime.now(),'T',f"""
                                    CREATE TABLE IF NOT EXISTS """+ '"'  + coorp_id_create_structure+ '"'  + """.products
                                        (
                                        sequence SERIAL PRIMARY KEY,
                                        id integer,
                                        name text COLLATE pg_catalog."default",
                                        departmentid integer,
                                        categoryid integer,
                                        brandid integer,
                                        linkid text COLLATE pg_catalog."default",
                                        refid text COLLATE pg_catalog."default",
                                        isvisible boolean,
                                        description text COLLATE pg_catalog."default",
                                        descriptionshort text COLLATE pg_catalog."default",
                                        releasedate date,
                                        keywords text COLLATE pg_catalog."default",
                                        title text COLLATE pg_catalog."default",
                                        isactive boolean,
                                        taxcode text COLLATE pg_catalog."default",
                                        metatagdescription text COLLATE pg_catalog."default",
                                        supplierid integer,
                                        showwithoutstock boolean,
                                        adwordsremarketingcode text COLLATE pg_catalog."default",
                                        lomadeecampaigncode text COLLATE pg_catalog."default",
                                        score text COLLATE pg_catalog."default",
                                        data_insercao timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
                                        CONSTRAINT constraint_products_pkey_id UNIQUE (id)
                                        ) TABLESPACE pg_default; 
                                        CREATE INDEX idx_products ON """ + '"'  + coorp_id_create_structure+ '"'  + """.products USING btree (id);
                                        ALTER TABLE IF EXISTS """ + '"'  + coorp_id_create_structure+ '"'  + """.products
                                        OWNER to adminuserpggemdatadev;
                                        """ ,'10','0','0','products') 
            
                          #query para tabela skus
        write_query_structure(coorp_id_create_structure,datetime.now(),'T',f"""
                                    CREATE TABLE IF NOT EXISTS """+ '"'  + coorp_id_create_structure+ '"'  + """.skus
                                        (
                                        sequence SERIAL PRIMARY KEY,
                                        id integer,
                                        productid integer,
                                        namecomplete text COLLATE pg_catalog."default",
                                        complementname text COLLATE pg_catalog."default",
                                        productname text COLLATE pg_catalog."default",
                                        productdescription text COLLATE pg_catalog."default",
                                        productrefid text COLLATE pg_catalog."default",
                                        taxcode text COLLATE pg_catalog."default",
                                        skuname text COLLATE pg_catalog."default",
                                        isactive boolean,
                                        istransported boolean,
                                        isinventoried boolean,
                                        isgiftcardrecharge boolean,
                                        imageurl text COLLATE pg_catalog."default",
                                        detailurl text COLLATE pg_catalog."default",
                                        cscidentification text COLLATE pg_catalog."default",
                                        brandid text COLLATE pg_catalog."default",
                                        brandname text COLLATE pg_catalog."default",
                                        isbrandactive boolean,
                                        dimension jsonb,
                                        realdimension jsonb,
                                        manufacturercode text COLLATE pg_catalog."default",
                                        iskit boolean,
                                        kititems jsonb,
                                        services jsonb,
                                        categories jsonb,
                                        categoriesfullpath jsonb,
                                        attachments jsonb,
                                        collections jsonb,
                                        skusellers jsonb,
                                        saleschannels jsonb,
                                        images jsonb,
                                        videos jsonb,
                                        skuspecifications jsonb,
                                        productspecifications jsonb,
                                        productclustersids text COLLATE pg_catalog."default",
                                        positionsinclusters jsonb,
                                        productclusternames jsonb,
                                        productclusterhighlights jsonb,
                                        productcategoryids text COLLATE pg_catalog."default",
                                        isdirectcategoryactive boolean,
                                        productglobalcategoryid integer,
                                        productcategories jsonb,
                                        commercialconditionid integer,
                                        rewardvalue double precision,
                                        alternateids jsonb,
                                        alternateidvalues jsonb,
                                        estimateddatearrival timestamp without time zone,
                                        measurementunit text COLLATE pg_catalog."default",
                                        unitmultiplier double precision,
                                        informationsource text COLLATE pg_catalog."default",
                                        modaltype text COLLATE pg_catalog."default",
                                        keywords text COLLATE pg_catalog."default",
                                        releasedate timestamp without time zone,
                                        productisvisible boolean,
                                        showifnotavailable boolean,
                                        isproductactive boolean,
                                        productfinalscore double precision,
                                        data_insercao timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
                                        CONSTRAINT constraint_sku_id UNIQUE (id)
                                        ) TABLESPACE pg_default; 
                                        CREATE INDEX idx_skus_id ON """ + '"'  + coorp_id_create_structure+ '"'  + """.skus USING btree (id);
                                        ALTER TABLE IF EXISTS """ + '"'  + coorp_id_create_structure+ '"'  + """.skus
                                        OWNER to adminuserpggemdatadev;
                                        """ ,'11','0','0','skus') 
            

    except Exception as e:
        print(f"Erro ao inserir ma tabela integration_create_structure : {e}")





#aqui é onde executa de fato as querys para criar o schema, tabela e etc .. 
def execute_query_structure ():
    try:
        query = """select  code_sql,id_sequential_structure from integration_create_structure
                    where end_status_execution = '0' and  id_integration = '"""  + coorp_id_create_structure  + """' 
                    order by id_integration,sequence_execution ;"""
        query_struture = WriteJsonToPostgres(coorp_conection_info, query, 'integration_create_structure').query()
        
        for query_execute in query_struture[0]:
            
            #print(query_execute[1])
            #EXECUNTANDO A QUERY 
            retorno=WriteJsonToPostgres(data_conection_info, query_execute[0]).execute_data_query()
           
            #EXECUNTANDO A QUERY 
            if retorno == True : 
                update_query =""" UPDATE integration_create_structure 
                                SET end_status_execution = '1', 
                                end_create_date ='""" + str(datetime.now()) + """' 
                                WHERE id_sequential_structure = """+ str(query_execute[1]) + ""
                WriteJsonToPostgres(coorp_conection_info, update_query ).execute_data_query()
            else:  
             break

            #update_query = {'end_status_execution' :  '0' }
            #WriteJsonToPostgres(coorp_conection_info, update_query,'integration_create_structure','id_sequential_structure' ).upsert_data()
        query_check = """select  id_sequential_structure from integration_create_structure
                    where end_status_execution = '0' and  id_integration = '"""  + coorp_id_create_structure  + """' 
                                """
        returno_check = WriteJsonToPostgres(coorp_conection_info, query_check, 'integration_create_structure').query()
   
        if not returno_check[0] and  query_struture[0] != []  :
            update_query =""" UPDATE integrations_integration 
                                SET infra_create_status = 'true', 
                                infra_create_date ='""" + str(datetime.now()) + """' 
                                WHERE id = '"""+ coorp_id_create_structure + "'"
            WriteJsonToPostgres(coorp_conection_info, update_query).execute_data_query()
        else: 
            if  not returno_check[0] :
                print(f"Não executou toda estrutura (tabela integration_create_structure) para cliente " + coorp_id_create_structure) 
            else: 
                print(f"Não possui linha na tabela integration_create_structure para executar do cliente " + coorp_id_create_structure)

    except Exception as e:
        print(f"Erro no processo de execucao(execute_query_structure) : {e}")      





def set_globals(api_info, data_conection,coorp_conection, **kwargs):

    global api_conection_info
    api_conection_info = api_info
    
    global data_conection_info
    data_conection_info = data_conection
    
    global coorp_conection_info
    coorp_conection_info = coorp_conection
    

    global coorp_id_create_structure
    coorp_id_create_structure = data_conection['schema']
    
    #precisa colocar um log sobre isso 
    #inserir as linhas com as query que deveram ser executadas no postgree
    #white_integration_create_structure() 
    
    #executa as linhas 
    execute_query_structure()

if __name__ == "__main__":
    #print(data_conection_info)
    
    white_integration_create_structure()


    