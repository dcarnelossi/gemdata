import json
import os
import psycopg2
from psycopg2 import sql

from datetime import datetime

import psycopg2.extensions

from vtex.modules import dbpgconn

#self.connection = PostgresConnection(connection_info)

#psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
#psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

# Substitua essas informações pelas do seu banco de dados
db_params = {
    'host': '20.98.214.213',
    'database': 'db-vetex-dev-00',
    'user': 'adminuserpggemdatadev',
    'password': 'qgfC64psgk7CCveWRFHAPCQR0F3DPzdxIUW7uD2HaHxuG0MoemnvpHYlCeM5',
    'port': '5432',
}




# Função para criar uma conexão com o PostgreSQL
def criar_conexao():
    try:
        # Conecta ao banco de dados
        conexao = psycopg2.connect(**db_params)
        print("Conexão bem-sucedida!")
        return conexao
    except Exception as e:
        print(f"Erro ao conectar ao PostgreSQL: {e}")
        return None
    
#aqui cadastra um novo cliente  e comeca o processo de criação
#esse aqui vai vim de um cadastro do cliente no sistema    
#cadastrar_novo_cliente('mahogany',333333','1')    
def cadastrar_novo_cliente(nome_cliente,cnpj,status_cliente):
    try:
        
        conexao = criar_conexao()
        cur = conexao.cursor()
        query = "INSERT INTO corporativo.cliente (nome_cliente, cnpj,status_cliente)  VALUES (%s,%s,%s)"
        
        # Executa a consulta
        cur.execute(query,(nome_cliente,cnpj,status_cliente))
        conexao.commit()
        return "Cliente criado"

    except Exception as e:
        print(f"Erro ao criar cliente: {e}")

    finally:
        # Fecha o cursor e a conexão
        cur.close()
        cur.connection.close()
        print("Conexão fechada.")


#cadastra uma nova empresa 
#esse aqui vai vim de um cadastro de empresa depois que o cara fez o cadastro(logi) no sistema  
#o Id_cliente tem que vim do sistema
#cadastrar_nova_empresa('1',mahogany','33333')                 
def cadastrar_nova_empresa(id_cliente,nome_empresa,cnpj_empresa):
    try:
        
        conexao = criar_conexao()
        cur = conexao.cursor()
        query = "INSERT INTO corporativo.empresa (id_cliente,nome_empresa, cnpj_empresa)  VALUES (%s,%s,%s)"
        
        # Executa a consulta
        cur.execute(query,(id_cliente,nome_empresa,cnpj_empresa))
        conexao.commit()
        return "Empresa criado"

    except Exception as e:
        print(f"Erro ao criar empresa: {e}")

    finally:
        # Fecha o cursor e a conexão
        cur.close()
        cur.connection.close()
        print("Conexão fechada.")


#cadastra uma nova api
#esse aqui vai vim de um cadastro de empresa depois que o cara fez o cadastro(logi) no sistema  
#o Id_cliente e id_empresa tem que vim do sistema         
def cadastrar_nova_api(hash_cadastro_api,id_cliente,id_empresa, data_criacao,status_executado,status_api):
    try:
        
        conexao = criar_conexao()
        cur = conexao.cursor()
        query = "INSERT INTO corporativo.cadastro_api (hash_cadastro_api,id_cliente,id_empresa, data_criacao,status_executado,status_api)  VALUES (%s,%s,%s,%s,%s,%s)"
        
        # Executa a consulta
        cur.execute(query,(hash_cadastro_api,id_cliente,id_empresa, data_criacao,status_executado,status_api))
        conexao.commit()
        return "API criado"

    except Exception as e:
        print(f"Erro ao criar API: {e}")

    finally:
        # Fecha o cursor e a conexão
        cur.close()
        cur.connection.close()
        print("Conexão fechada.")


#inserir_query_estrutura = aqui é onde tem o comando para inserir na tabela corporativo.criar_estrutura 
def inserir_query_estrutura(hash_cadastro_api,data_criacao,tipo_pg,codigo_sql,sequencia_execucao,status_executado,gerar_json,nome_sql_json):
    try:
        
        conexao = criar_conexao()
        cur = conexao.cursor()
        query = "INSERT INTO corporativo.criar_estrutura (hash_cadastro_api,data_criacao,tipo_pg,codigo_sql,sequencia_execucao,status_executado,gerar_json,nome_sql_json)  VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
        # Executa a consulta
        cur.execute(query,(hash_cadastro_api,data_criacao,tipo_pg,codigo_sql,sequencia_execucao,status_executado,gerar_json,nome_sql_json))
        conexao.commit()
        return "Query criado"

    except Exception as e:
        return e

    finally:
        # Fecha o cursor e a conexão
        cur.close()
        cur.connection.close()
        print("Conexão fechada.")


# aqui é para executar qualquer consulta 
def executar_consulta(consulta):
    try:
        # Cria um cursor para executar a consulta
        conexao = criar_conexao()
        cursor = conexao.cursor()

        # Executa a consulta
        cursor.execute(consulta)

        # Obtém os resultados
        resultados = cursor.fetchall()

        return resultados

        #resultados_lista = [list(row) for row in resultados]
        
        #return resultados_lista

    except Exception as e:
        print(f"Erro ao executar a consulta: {e}")

    finally:
        # Fecha o cursor e a conexão
        cursor.close()
        conexao.close()
        print("Conexão fechada.")


# aqui é para executar qualquer update, create e etc .. 
def executar_query_commit(query):
    try:
        conexao = criar_conexao()
        cur = conexao.cursor()
        #print(query)
        # Executa a consulta
        cur.execute(query)
        
        conexao.commit()
        return "Query executada"

    except Exception as e:
        print(f"Erro ao executar query: {e}")

    finally:
        # Fecha o cursor e a conexão
        cur.close()
        cur.connection.close()
        print("Conexão fechada.")
  

# aqui é onde cadastra de fato todaas as query 
#aqui é onde voce tem que colocar todas as query para criaçao do schema ... só cadastra dentro da tabela corporativo.criar_estrutura
def cadastrar_query_estrutura ():
    try:
        api=executar_consulta("""select  hash_cadastro_api from corporativo.cadastro_api where status_executado = '0' and status_api ='1' """)
        for hashapi in api:
            #query para criar schema 
            inserir_query_estrutura(hashapi[0],datetime.now(),'S','CREATE schema IF NOT EXISTS "'+ hashapi[0] + '"','1','0','0','schema') 
            #query para tabela brands
            inserir_query_estrutura(hashapi[0],datetime.now(),'T',f"""
                                    CREATE TABLE IF NOT EXISTS """+ '"'  + hashapi[0] + '"'  + """.brands
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
                                        ALTER TABLE IF EXISTS """ + '"'  + hashapi[0] + '"'  + """.brands
                                        OWNER to adminuserpggemdatadev;
                                        """ ,'2','0','0','brands') 
             #query para tabela categories
            inserir_query_estrutura(hashapi[0],datetime.now(),'T',f"""
                                    CREATE TABLE IF NOT EXISTS """+ '"'  + hashapi[0] + '"'  + """.categories
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
                                        ALTER TABLE IF EXISTS """ + '"'  + hashapi[0] + '"'  + """.categories
                                        OWNER to adminuserpggemdatadev;
                                        """ ,'3','0','0','categories') 
              #query para tabela client_profile
            inserir_query_estrutura(hashapi[0],datetime.now(),'T',f"""
                                    CREATE TABLE IF NOT EXISTS """+ '"'  + hashapi[0] + '"'  + """.client_profile
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
                                        CREATE INDEX idx_client_profile_orderid ON """ + '"'  + hashapi[0] + '"'  + """.client_profile USING btree (orderid);
                                        ALTER TABLE IF EXISTS """ + '"'  + hashapi[0] + '"'  + """.client_profile
                                        OWNER to adminuserpggemdatadev;
                                        """ ,'4','0','0','client_profile')             
             #query para tabela orders
            inserir_query_estrutura(hashapi[0],datetime.now(),'T',f"""
                                    CREATE TABLE IF NOT EXISTS """+ '"'  + hashapi[0] + '"'  + """.orders
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
                                        ALTER TABLE IF EXISTS """ + '"'  + hashapi[0] + '"'  + """.orders
                                        OWNER to adminuserpggemdatadev;
                                        CREATE INDEX IF NOT EXISTS "idx_orders_orderid"
                                                ON """ + '"'  + hashapi[0] + '"'  + """.orders USING btree
                                                (orderid COLLATE pg_catalog."default" ASC NULLS LAST)
                                                TABLESPACE pg_default;
                                        """ ,'5','0','0','orders') 
                          #query para tabela orders_items
            inserir_query_estrutura(hashapi[0],datetime.now(),'T',f"""
                                    CREATE TABLE IF NOT EXISTS """+ '"'  + hashapi[0] + '"'  + """.orders_items
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
                                        CREATE INDEX idx_orders_items_orderid ON """ + '"'  + hashapi[0] + '"'  + """.orders_items USING btree (orderid);
                                        ALTER TABLE IF EXISTS """ + '"'  + hashapi[0] + '"'  + """.orders_items
                                        OWNER to adminuserpggemdatadev;
                                        """ ,'6','0','0','orders_items') 
                          #query para tabela orders_list
            inserir_query_estrutura(hashapi[0],datetime.now(),'T',f"""
                                    CREATE TABLE IF NOT EXISTS """+ '"'  + hashapi[0] + '"'  + """.orders_list
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
                                         CREATE INDEX idx_orders_list_orderid ON """ + '"'  + hashapi[0] + '"'  + """.orders_list USING btree (orderid);
                                        ALTER TABLE IF EXISTS """ + '"'  + hashapi[0] + '"'  + """.orders_list
                                        OWNER to adminuserpggemdatadev;
                                        """ ,'7','0','0','orders_list') 
            
              #query para tabela orders_shippingdata
            inserir_query_estrutura(hashapi[0],datetime.now(),'T',f"""
                                    CREATE TABLE IF NOT EXISTS """+ '"'  + hashapi[0] + '"'  + """.orders_shippingdata
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
                                        ALTER TABLE IF EXISTS """ + '"'  + hashapi[0] + '"'  + """.orders_shippingdata
                                        OWNER to adminuserpggemdatadev;
                                        CREATE INDEX IF NOT EXISTS idx_orders_shippingdata
                                        ON """ + '"'  + hashapi[0] + '"'  + """.orders_shippingdata USING btree
                                        (orderid COLLATE pg_catalog."default" ASC NULLS LAST)
                                        TABLESPACE pg_default;
                                        """ ,'8','0','0','orders_shippingdata')           
                          #query para tabela orders_totals
            inserir_query_estrutura(hashapi[0],datetime.now(),'T',f"""
                                    CREATE TABLE IF NOT EXISTS """+ '"'  + hashapi[0] + '"'  + """.orders_totals
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
                                        CREATE INDEX idx_orders_totals ON """ + '"'  + hashapi[0] + '"'  + """.orders_totals USING btree (orderid);
                                        ALTER TABLE IF EXISTS """ + '"'  + hashapi[0] + '"'  + """.orders_totals
                                        OWNER to adminuserpggemdatadev;
                                        """ ,'9','0','0','orders_totals')   
            
                          #query para tabela products
            inserir_query_estrutura(hashapi[0],datetime.now(),'T',f"""
                                    CREATE TABLE IF NOT EXISTS """+ '"'  + hashapi[0] + '"'  + """.products
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
                                        CREATE INDEX idx_products ON """ + '"'  + hashapi[0] + '"'  + """.products USING btree (id);
                                        ALTER TABLE IF EXISTS """ + '"'  + hashapi[0] + '"'  + """.products
                                        OWNER to adminuserpggemdatadev;
                                        """ ,'10','0','0','products') 
            
                          #query para tabela skus
            inserir_query_estrutura(hashapi[0],datetime.now(),'T',f"""
                                    CREATE TABLE IF NOT EXISTS """+ '"'  + hashapi[0] + '"'  + """.skus
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
                                        CREATE INDEX idx_skus_id ON """ + '"'  + hashapi[0] + '"'  + """.skus USING btree (id);
                                        ALTER TABLE IF EXISTS """ + '"'  + hashapi[0] + '"'  + """.skus
                                        OWNER to adminuserpggemdatadev;
                                        """ ,'11','0','0','skus') 
            

            

              #query padrao para tabela 
          #  inserir_query_estrutura(hashapi[0],datetime.now(),'T',f"""
          #                          CREATE TABLE IF NOT EXISTS """+ '"'  + hashapi[0] + '"'  + """.tab
          #                              (
          #                              ) TABLESPACE pg_default; 
          #                              ALTER TABLE IF EXISTS """ + '"'  + hashapi[0] + '"'  + """.tab
          #                              OWNER to adminuserpggemdatadev;
          #                              """ ,'12','0','0','tab') 
            #query para criar view 
          #  inserir_query_estrutura(hashapi[0],datetime.now(),'V','CREATE OR REPLACE VIEW "' +  hashapi[0] + '".json_teste  as ( select name,id from "' + hashapi[0]  + '".brands b)','3','0','1','json_teste') 
            #update falando que foi inserido as query dentro da tabela 
            executar_query_commit(""" UPDATE corporativo.cadastro_api SET status_executado = '1', data_execucao ='""" + str(datetime.now()) + """' WHERE hash_cadastro_api = '"""+ hashapi[0] + "'")
            


    #padrao de criacao da tebale operacioal_bd
 #     hash_cadastro_api,data_criacao,tipo_pg,codigo_sql,sequencia_execucao,status_executado,status_executar_dash_diario

    except Exception as e:
        print(f"Erro ao inserir na tabela operacional bd: {e}")




#aqui é onde executa de fato as querys para criar o schema, tabela e etc .. 
def executar_query_estrutura ():
    try:
        api=executar_consulta("""select  id_operacional_bd,codigo_sql from corporativo.criar_estrutura where status_executado = '0' order by hash_cadastro_api,id_operacional_bd  """)
        for idoperacional in api:
            #EXECUNTANDO A QUERY 
            executar_query_commit(idoperacional[1])
            #EXECUNTANDO A QUERY 
            executar_query_commit(""" UPDATE corporativo.criar_estrutura SET status_executado = '1', data_execucao ='""" + str(datetime.now()) + """' WHERE id_operacional_bd = """+ str(idoperacional[0]) + "")
            
    #padrao de criacao da tebale operacioal_bd
 #     hash_cadastro_api,data_criacao,tipo_pg,codigo_sql,sequencia_execucao,status_executado,status_executar_dash_diario

    except Exception as e:
        print(f"Erro ao inserir as query na tabela  corporativo.criar_estrutura : {e}")

#cadastrar_query_estrutura()
#executar_query_estrutura ()

#cliente = nome da empresa que fiz o login
#cadastrar_novo_cliente('mahogany','333333','1')
#empresa = no momento vai ser o mesmo que cliente , mas no futuro pode ser que tenhoa 1 cliente para n empresas        
#cadastrar_nova_empresa('1','mahogany','33333')        
#API = cadastrar a conexao do VTEX e gerar o hash para usar como ID serial (aqui em baixo eu cadastrei dois hash diferente para 1 empresa/cliente) 
#cadastra na tabel corporativo.cadastro_api      
#cadastrar_nova_api ('4jasdhasdgaeayrhaejdakjddas','1','1', datetime.now(),'0','1')
#cadastrar_nova_api ('1jasdhasdgaeayrhaejdakjddas','1','1', datetime.now(),'0','1') 
#aqui cadastra todas as query (ainda não executa as querys, apenas cadastra em uma tabela corporativo.operacional_bd)        
#cadastrar_query_estrutura()
#aqui sai executando todas as querys
#executar_query_estrutura ()



#aqui é onde executa de fato as querys para criar o schema, tabela e etc .. 
#todas as view que tem "json_" ira executar o processo de json         




####################################################################################################################################################
##DAQUI EM DIANTE É PARA FAZER O JSON PARA OS GRAFICOS  

 #CRIAR O DIRETORIO/PASTA       
 
def criando_pasta_json(empresa,grafico):
    # Diretório onde deseja salvar o arquivo
    diretorio = 'C:/Users/gabri/OneDrive/Área de Trabalho/' + empresa
    arquivo = diretorio + '/' + grafico

    # Garantir que o diretório exista, se não, criar
    try: 
        if not os.path.exists(diretorio):
            os.makedirs(diretorio)

    except Exception as e:
        print(f"Erro ao criar pasta: {e}")        

    #Vendo se existe o arquivo para deletar 
    try: 
        if os.path.exists(arquivo):
             os.remove(arquivo)
           
    except Exception as e:
        print(f"Erro ao deletar arquivo: {e}")        
        
    return arquivo   

# Exemplo de execução de uma consulta para json 
def executar_query_json(consulta,args=(), one= False):
    try:
        # Cria um cursor para executar a consulta
        #exemplo isso aqui criar conexao é DEF
        cur = criar_conexao().cursor()
        # Executa a consulta
        cur.execute(consulta, args)
        r = [dict((cur.description[i][0], value) \
               for i, value in enumerate(row)) for row in cur.fetchall()]
        cur.connection.close()
        return (r[0] if r else None) if one else r
            
    except Exception as e:
        print(f"Erro ao executar a consulta: {e}")

    finally:
        # Fecha o cursor e a conexão
        cur.close()
        cur.connection.close()
        print("Conexão fechada.")




#aqui é onde executa de fato as querys para criar o schema, tabela e etc .. 
def criar_query_json_arquivo ():
    try:
        lista_json=executar_consulta("""select  id_operacional_bd,hash_cadastro_api,nome_sql_json from corporativo.criar_estrutura where status_executado = '1' and gerar_json ='1' order by id_operacional_bd,hash_cadastro_api """)
        for idoperacional in lista_json:
            data_criacao =str(datetime.now()) 
            executar_query_commit(' INSERT INTO  corporativo.criacao_json_log(id_operacional_bd,hash_cadastro_api,data_criacao,nome_sql_json) VALUES ('+ str(idoperacional[0]) + ','"'" + idoperacional[1] + "'"','"'" + data_criacao + "'"','"'" + idoperacional[2] + "')" )
    
            caminho=criando_pasta_json(idoperacional[1], idoperacional[2] + '.json')
            query_json=executar_query_json('select * from "' + idoperacional[1]  + '".'  + idoperacional[2])
            

            with open(caminho, 'w') as file:
                json.dump(query_json, file)

            executar_query_commit(""" UPDATE corporativo.criacao_json_log SET status_executado = '1', data_execucao ='""" + str(datetime.now()) + """' WHERE id_operacional_bd = """+ str(idoperacional[0]) + " and data_criacao = '" + data_criacao + "'")
    
            #EXECUNTANDO A QUERY
            #executar_query_commit(""" UPDATE corporativo.criar_estrutura SET status_executado = '1', data_execucao ='""" + str(datetime.now()) + """' WHERE id_operacional_bd = """+ str(idoperacional[0]) + "")
            
    #padrao de criacao da tebale operacioal_bd
 #     hash_cadastro_api,data_criacao,tipo_pg,codigo_sql,sequencia_execucao,status_executado,status_executar_dash_diario

    except Exception as e:
        print(f"Erro ao criar json : {e}")



#criar_query_json_arquivo()

#teste_caminho= diretorio_json('teste1','a.json')
#print(teste_caminho)
#my_query = executar_consulta("select cast(round((cast(oi.price as numeric)/100),	2 ) as text) as price from mahogany.orders_items oi limit 1 ")



#print (my_query)

#with open(teste_caminho, 'w') as file:
#    json.dump(my_query, file)
