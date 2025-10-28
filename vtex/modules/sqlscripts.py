def vtexsqlscripts(schema, user):
    scripts = f"""

    CREATE SCHEMA IF NOT EXISTS "{schema}";

    CREATE TABLE IF NOT EXISTS "{schema}".brands
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
    )

    TABLESPACE pg_default;

    ALTER TABLE IF EXISTS "{schema}".brands
        OWNER to {user};


    CREATE TABLE IF NOT EXISTS "{schema}".categories
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
        treepathids bigint,
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

    )

    TABLESPACE pg_default;

    ALTER TABLE IF EXISTS "{schema}".categories
        OWNER to {user};


    CREATE TABLE IF NOT EXISTS "{schema}".client_profile
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
    )

    TABLESPACE pg_default;

    CREATE INDEX IF NOT EXISTS idx_client_profile_orderid ON "{schema}".client_profile USING btree (orderid);

    ALTER TABLE IF EXISTS "{schema}".client_profile
        OWNER to {user};


    CREATE TABLE IF NOT EXISTS "{schema}".orders
    (
        sequence SERIAL PRIMARY KEY,
        orderid varchar(50) COLLATE pg_catalog."default",
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
        cancellationrequests character varying COLLATE pg_catalog."default",
        CONSTRAINT constraint_orders_orderid UNIQUE (orderid)
    )

    TABLESPACE pg_default;

    ALTER TABLE IF EXISTS "{schema}".orders
        OWNER to {user};



    CREATE TABLE IF NOT EXISTS "{schema}".realtime_vtex_orders
    (
        sequence SERIAL PRIMARY KEY,
        orderid varchar(50) COLLATE pg_catalog."default",
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
        cancellationrequests character varying COLLATE pg_catalog."default",
        CONSTRAINT constraint_realtime_vtex_orders_orderid UNIQUE (orderid)
    )

    TABLESPACE pg_default;

    ALTER TABLE IF EXISTS "{schema}".realtime_vtex_orders
        OWNER to {user};



    CREATE TABLE IF NOT EXISTS "{schema}".orders_items
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
        manualpriceappliedby character varying COLLATE pg_catalog."default",
        CONSTRAINT constraint_orders_items_orderid UNIQUE (uniqueid)
    )

    TABLESPACE pg_default;

    CREATE INDEX IF NOT EXISTS idx_orders_items_orderid ON "{schema}".orders_items USING btree (orderid);


    ALTER TABLE IF EXISTS "{schema}".orders_items
        OWNER to {user};


    CREATE TABLE IF NOT EXISTS "{schema}".orders_list
    (
        sequence_id SERIAL PRIMARY KEY,
        orderid text COLLATE pg_catalog."default",
        creationdate timestamp without time zone,
        clientname text COLLATE pg_catalog."default",
        items jsonb,
        totalvalue numeric(14,2),
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
        is_change BOOLEAN DEFAULT TRUE,
        CONSTRAINT constraint_orders_list_orderid UNIQUE (orderid)

    )

    TABLESPACE pg_default;

    CREATE INDEX IF NOT EXISTS idx_orders_list_orderid ON "{schema}".orders_list USING btree (orderid);

    ALTER TABLE IF EXISTS "{schema}".orders_list
        OWNER to {user};

        

    CREATE TABLE IF NOT EXISTS "{schema}".orders_list_daily
    (
        sequence_id SERIAL PRIMARY KEY,
        orderid text COLLATE pg_catalog."default",
        creationdate timestamp without time zone,
        clientname text COLLATE pg_catalog."default",
        items jsonb,
        totalvalue numeric(14,2),
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
        CONSTRAINT constraint_orders_list_daily_orderid UNIQUE (orderid)

    )

    TABLESPACE pg_default;

    CREATE INDEX IF NOT EXISTS idx_orders_list_daily_orderid ON "{schema}".orders_list_daily USING btree (orderid);

    ALTER TABLE IF EXISTS "{schema}".orders_list_daily
        OWNER to {user};



    CREATE TABLE IF NOT EXISTS "{schema}".orders_shippingdata
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
    )

    TABLESPACE pg_default;

    ALTER TABLE IF EXISTS "{schema}".orders_shippingdata
        OWNER to {user};


    CREATE INDEX IF NOT EXISTS idx_orders_shippingdata
        ON "{schema}".orders_shippingdata USING btree
        (orderid COLLATE pg_catalog."default" ASC NULLS LAST)
        TABLESPACE pg_default;


    CREATE TABLE IF NOT EXISTS "{schema}".orders_totals
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
    )

    TABLESPACE pg_default;

    CREATE INDEX IF NOT EXISTS idx_orders_totals ON "{schema}".orders_totals USING btree (orderid);


    ALTER TABLE IF EXISTS "{schema}".orders_totals
        OWNER to {user};



    CREATE TABLE IF NOT EXISTS "{schema}".products
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
    )

    TABLESPACE pg_default;

    CREATE INDEX IF NOT EXISTS idx_products ON "{schema}".products USING btree (id);

    ALTER TABLE IF EXISTS "{schema}".products
        OWNER to {user};



    CREATE TABLE IF NOT EXISTS "{schema}".skus
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
    )

    TABLESPACE pg_default;

    CREATE INDEX IF NOT EXISTS idx_skus_id ON "{schema}".products USING btree (id);

    ALTER TABLE IF EXISTS "{schema}".skus
        OWNER to {user};


    CREATE OR REPLACE VIEW "{schema}".counts_view AS
        SELECT
            (SELECT count(*) FROM "{schema}".brands) AS brand_count,
            (SELECT count(*) FROM "{schema}".categories) AS category_count,
            (SELECT count(*) FROM "{schema}".client_profile) AS client_profile_count,
            (SELECT count(*) FROM "{schema}".orders) AS orders_count,
            (SELECT count(*) FROM "{schema}".orders_items) AS orders_items_count,
            (SELECT count(*) FROM "{schema}".orders_list) AS orders_list_count,
            (SELECT count(*) FROM "{schema}".orders_shippingdata) AS orders_shippingdata_count,
            (SELECT count(*) FROM "{schema}".orders_totals) AS orders_totals_count,
            (SELECT count(*) FROM "{schema}".products) AS products_count,
            (SELECT count(*) FROM "{schema}".skus) AS skus_count;


    """

    return scripts


def shopifysqlscripts(schema, user):
    scripts = f"""

    CREATE SCHEMA IF NOT EXISTS  "{schema}";
   
 
    CREATE TABLE IF NOT EXISTS  "{schema}".shopify_orders
    (
    id SERIAL PRIMARY key,
    orderid bigint,
    idshopify VARCHAR(200) , 
    name VARCHAR(150),
    email VARCHAR(200),
    createdat TIMESTAMP,
    updatedat TIMESTAMP,
    closedat TIMESTAMP,
    cancelledat TIMESTAMP,
    cancelreason VARCHAR(50),
    currencycode VARCHAR(10),
    currentsubtotalprice NUMERIC(10, 2),
    currenttotaldiscounts NUMERIC(10, 2),
    currenttotalprice NUMERIC(10, 2),
    currenttotaltax NUMERIC(10, 2),
    customerlocale VARCHAR(30),
    displayfinancialstatus VARCHAR(50),
    totalweight INTEGER,
    totalshippingprice NUMERIC(10, 2),
    totaltax NUMERIC(10, 2),
    totalprice NUMERIC(10, 2),
    shippingfirstname VARCHAR(150),
    --shippinglastname VARCHAR(255),
   -- shippingcompany VARCHAR(50),
    shippingcity VARCHAR(150),
    shippingzip VARCHAR(50),
    shippingprovincecode VARCHAR(10),
    shippingcountrycode VARCHAR(10),
    --billingcompany VARCHAR(50),
    --billingaddress1 VARCHAR(50),
    --billingaddress2 TEXT,
    billingcity VARCHAR(100),
    billingzip VARCHAR(50),
    billingprovincecode VARCHAR(10),
    billingcountrycode VARCHAR(10),
    channelname VARCHAR(150),
    subchannelname VARCHAR(150),
    data_insercao timestamp DEFAULT CURRENT_TIMESTAMP null,
    CONSTRAINT constraint_orders_shopifyorderid UNIQUE (orderid)
    )

    TABLESPACE pg_default;

    ALTER TABLE IF EXISTS  "{schema}".shopify_orders
        OWNER to {user};

    CREATE INDEX IF NOT EXISTS idx_shopify_orders_orderid ON  "{schema}".shopify_orders USING btree (orderid);


    CREATE TABLE IF NOT EXISTS  "{schema}".shopify_orders_items
    (
    	id SERIAL PRIMARY key,
        orderid BIGINT ,
        iditemshopify VARCHAR(200),
        title VARCHAR(255),
        quantity INTEGER,
        originalunitprice NUMERIC(10, 2),
        originalunitcurrencycode VARCHAR(50),
        variantid VARCHAR(255),
        varianttitle TEXT,
        variantsku VARCHAR(200),
        variantprice NUMERIC(10, 2),
        productid VARCHAR(200),
        producttitle TEXT,
        productvendor VARCHAR(100),
        producttype VARCHAR(200),
        totaldiscountamount NUMERIC(10, 2),
        data_insercao timestamp DEFAULT CURRENT_TIMESTAMP null,
    	CONSTRAINT constraint_shopify_orders_items_id UNIQUE (iditemshopify)
    )

    TABLESPACE pg_default;

    ALTER TABLE IF EXISTS  "{schema}".shopify_orders_items
        OWNER to {user};


   CREATE INDEX IF NOT EXISTS idx_orders_item_combined ON "{schema}".shopify_orders_items USING btree (orderid);
  
   
   
     CREATE TABLE IF NOT EXISTS  "{schema}".shopify_orders_payment
    (
	id SERIAL PRIMARY KEY,                 
	orderid bigint, 
	idpaymentshopify VARCHAR(255)  ,  
	transactionid VARCHAR(255),           
    kind VARCHAR(50),
    paymentMethod varchar(50),                   
    status VARCHAR(50),                    
    amount NUMERIC(10, 2),                
    currencycode VARCHAR(10),             
    gateway VARCHAR(100),                  
    createdat TIMESTAMP,                  
    data_insercao timestamp DEFAULT CURRENT_TIMESTAMP null,
    CONSTRAINT constraint_shopify_orders_payment_id UNIQUE (idpaymentshopify)
        )

    TABLESPACE pg_default;

    ALTER TABLE IF EXISTS  "{schema}".shopify_orders_payment
        OWNER to {user};
       
    CREATE INDEX IF NOT EXISTS idx_orders_payment_combined ON "{schema}".shopify_orders_payment USING btree (orderid);
  
    
    CREATE TABLE IF NOT EXISTS "{schema}".shopify_gemdata_categoria(
        idcategoriagemdata serial4 NOT NULL,
        nomecategoria varchar(200) NULL,
        data_insercao timestamp DEFAULT CURRENT_TIMESTAMP NULL,
        CONSTRAINT shopify_gemdata_categoria_pkey PRIMARY KEY (idcategoriagemdata)
    )
    TABLESPACE pg_default;

    ALTER TABLE IF EXISTS  "{schema}".shopify_gemdata_categoria
    OWNER to {user};

    CREATE INDEX IF NOT EXISTS  idx_shopify_gemdata_categoria_name ON "{schema}".shopify_gemdata_categoria USING btree (nomecategoria);	

    
    
    CREATE TABLE IF NOT EXISTS  "{schema}".realtime_shopify_orders
    (
    id SERIAL PRIMARY key,
    orderid bigint,
    idshopify VARCHAR(200) , 
    name VARCHAR(150),
    email VARCHAR(200),
    createdat TIMESTAMP,
    updatedat TIMESTAMP,
    closedat TIMESTAMP,
    cancelledat TIMESTAMP,
    cancelreason VARCHAR(50),
    currencycode VARCHAR(10),
    currentsubtotalprice NUMERIC(10, 2),
    currenttotaldiscounts NUMERIC(10, 2),
    currenttotalprice NUMERIC(10, 2),
    currenttotaltax NUMERIC(10, 2),
    customerlocale VARCHAR(30),
    displayfinancialstatus VARCHAR(50),
    totalweight INTEGER,
    totalshippingprice NUMERIC(10, 2),
    totaltax NUMERIC(10, 2),
    totalprice NUMERIC(10, 2),
    shippingfirstname VARCHAR(150),
    --shippinglastname VARCHAR(255),
   -- shippingcompany VARCHAR(50),
    shippingcity VARCHAR(150),
    shippingzip VARCHAR(50),
    shippingprovincecode VARCHAR(10),
    shippingcountrycode VARCHAR(10),
    --billingcompany VARCHAR(50),
    --billingaddress1 VARCHAR(50),
    --billingaddress2 TEXT,
    billingcity VARCHAR(100),
    billingzip VARCHAR(50),
    billingprovincecode VARCHAR(10),
    billingcountrycode VARCHAR(10),
    channelname VARCHAR(150),
    subchannelname VARCHAR(150),
    data_insercao timestamp DEFAULT CURRENT_TIMESTAMP null,
    CONSTRAINT constraint_realtime_orders_shopifyorderid UNIQUE (orderid)
    )

    TABLESPACE pg_default;

    ALTER TABLE IF EXISTS  "{schema}".realtime_shopify_orders
        OWNER to {user};


    """

    return scripts


def gasqlscripts(schema, user):
    scripts = f"""

     CREATE TABLE IF NOT EXISTS "{schema}".ga_sessions_users (
	creationdate timestamp not NULL,
	totalusers float NULL,
	newusers float null,
	sessions float NULL,
	engagedSessions float NULL,
	averageSessionDuration float NULL,
	bounceRate float NULL,
	purchaseRevenue float NULL,
	data_insercao timestamp DEFAULT CURRENT_TIMESTAMP null,
	 CONSTRAINT constraint_ga_sessions_users_unique UNIQUE (creationdate)
        );

    ALTER TABLE IF EXISTS  "{schema}".ga_sessions_users
    OWNER to {user};	

     CREATE TABLE IF NOT EXISTS  "{schema}".ga_engagement_event (
	creationdate timestamp not NULL,
	eventcount float NULL,
	engagementrate float null,
	userengagementduration float NULL,
	screenpageviews float NULL,
	data_insercao timestamp DEFAULT CURRENT_TIMESTAMP null,
	 CONSTRAINT constraint_ga_engagement_event_unique UNIQUE (creationdate)
    );	
    ALTER TABLE IF EXISTS  "{schema}".ga_engagement_event
    OWNER to {user};	

    CREATE TABLE IF NOT EXISTS "{schema}".ga_traffic(
	creationdate timestamp not NULL,
	sessionsource varchar(150) not NULL,
	sessionmedium varchar(150) not null,
	sessionsourcemedium varchar(150) not NULL,
	campaignname varchar(250) not NULL,
	sessions FLOAT,
	data_insercao timestamp DEFAULT CURRENT_TIMESTAMP null,
	CONSTRAINT constraint_ga_traffic_unique UNIQUE (creationdate, sessionsource, sessionmedium, sessionsourcemedium, campaignname)
	);	
    ALTER TABLE IF EXISTS  "{schema}".ga_traffic
    OWNER to {user};	

    

     CREATE TABLE IF NOT EXISTS "{schema}".ga_traffic_purchase(
        creationdate timestamp not NULL,
        sessionsource varchar(150) not NULL,
        sessionmedium varchar(150) not null,
        sessionsourcemedium varchar(150) not NULL,
        campaignname varchar(250) not NULL,
        sessions FLOAT,
        purchaseRevenue FLOAT,
        data_insercao timestamp DEFAULT CURRENT_TIMESTAMP null,
        CONSTRAINT constraint_ga_traffic_purchase_unique UNIQUE (creationdate, sessionsource, sessionmedium, sessionsourcemedium, campaignname)
        );	
    ALTER TABLE IF EXISTS  "{schema}".ga_traffic_purchase
    OWNER to {user};	

    CREATE TABLE IF NOT EXISTS "{schema}".ga_sessions_technology(
        creationdate timestamp not NULL,
        devicecategory varchar(150) not NULL,
        platform varchar(150) not null,
        browser varchar(150) not NULL,
        operatingsystem varchar(250) not NULL,
        sessions FLOAT,
        data_insercao timestamp DEFAULT CURRENT_TIMESTAMP null,
        CONSTRAINT constraint_ga_sessions_technology UNIQUE (creationdate, devicecategory, platform, browser, operatingsystem)
        );
    ALTER TABLE IF EXISTS  "{schema}".ga_sessions_technology
    OWNER to {user};	
    
     CREATE TABLE IF NOT EXISTS "{schema}".ga_sessions_geolocation(
	creationdate timestamp not NULL,
	country text not NULL,
	region text not null,
	city text not NULL,
	language text not NULL,
	sessions FLOAT,
	data_insercao timestamp DEFAULT CURRENT_TIMESTAMP null,
	CONSTRAINT constraint_ga_sessions_geolocation UNIQUE (creationdate, country, region, city, language)
	);	
    ALTER TABLE IF EXISTS  "{schema}".ga_sessions_geolocation
    OWNER to {user};

     CREATE TABLE IF NOT EXISTS "{schema}".ga_funnel_events(
	creationdate timestamp not NULL,
	event_name text not NULL,
	sessions float not NULL,
	data_insercao timestamp DEFAULT CURRENT_TIMESTAMP null,
	CONSTRAINT constraint_ga_funnel_events UNIQUE (creationdate, event_name)
	);	
    ALTER TABLE IF EXISTS  "{schema}".ga_funnel_events
    OWNER to {user};

     CREATE TABLE IF NOT EXISTS "{schema}".ga_funnel_events_region(
	creationdate timestamp not NULL,
	event_name text not NULL,
	city text not NULL,
	region text not NULL,
	sessions float not NULL,
	data_insercao timestamp DEFAULT CURRENT_TIMESTAMP null,
	CONSTRAINT constraint_ga_funnel_events_region UNIQUE (creationdate, event_name,city,region)
	);	
    ALTER TABLE IF EXISTS  "{schema}".ga_funnel_events_region
    OWNER to {user};

     CREATE TABLE IF NOT EXISTS "{schema}".ga_funnel_events_item(
	creationdate timestamp not NULL,
	itemid text not NULL,
	itemname text not NULL,
	itemsviewed float not NULL,
	itemspurchased float not NULL,
	itemsaddedtocart float not NULL,
	itemrevenue float not NULL,
	
	data_insercao timestamp DEFAULT CURRENT_TIMESTAMP null,
	CONSTRAINT constraint_ga_funnel_events_item UNIQUE (creationdate, itemid,itemname)
	);	  
    ALTER TABLE IF EXISTS  "{schema}".ga_funnel_events_item
    OWNER to {user};  

    """

    return scripts



def lojaintegradasqlscripts(schema, user):
    scripts = f"""

    CREATE SCHEMA IF NOT EXISTS "{schema}";
    
    CREATE TABLE IF NOT EXISTS "{schema}".lojaintegrada_list_products (
            id BIGINT PRIMARY KEY,
            name TEXT,
            alias TEXT,
            sku TEXT,
            active BOOLEAN,
            blocked BOOLEAN,
            removed BOOLEAN,
            type TEXT,
            url TEXT,
            description TEXT,
            gtin TEXT,
            mpn TEXT,
            ncm TEXT,
            external_id TEXT,
            youtube_url TEXT,
            categories JSONB,
            variations JSONB,
            grades JSONB,
            children JSONB,
            tags JSONB,
            resource_uri TEXT,
            seo_uri TEXT,
            data_insercao TIMESTAMPTZ DEFAULT now()
        );

    ALTER TABLE IF EXISTS  "{schema}".lojaintegrada_list_products
    OWNER to {user};	

    CREATE TABLE IF NOT EXISTS  "{schema}".lojaintegrada_products (
        id BIGINT PRIMARY KEY,
        sku TEXT,
        name TEXT,
        alias TEXT,
        type TEXT,
        url TEXT,
        description TEXT,
        gtin TEXT,
        mpn TEXT,
        ncm TEXT,
        external_id TEXT,
        youtube_url TEXT,
        active BOOLEAN,
        blocked BOOLEAN,
        removed BOOLEAN,
        used BOOLEAN,
        highlight BOOLEAN,
        brand TEXT,
        parent TEXT,
        main_image TEXT,
        resource_uri TEXT,
        seo_uri TEXT,
        cost_price NUMERIC,
        full_price NUMERIC,
        promotional_price NUMERIC,
        price_on_request BOOLEAN,
        stock_controlled BOOLEAN,
        stock_quantity NUMERIC,
        stock_in_stock_status INTEGER,
        stock_out_stock_status INTEGER,
        height NUMERIC,
        width NUMERIC,
        depth NUMERIC,
        weight NUMERIC,
        created_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ,
        tags TEXT,
        grades TEXT,
        images TEXT,
        children TEXT,
        categories TEXT,
        variations TEXT,
        data_insercao TIMESTAMPTZ DEFAULT now()
    );
    ALTER TABLE IF EXISTS  "{schema}".lojaintegrada_products
    OWNER to {user};	

     CREATE TABLE IF NOT EXISTS  "{schema}".lojaintegrada_categories (
            id BIGINT PRIMARY KEY,
            name TEXT,
            description TEXT,
            external_id TEXT,
            parent_category TEXT,
            url TEXT,
            resource_uri TEXT,
            seo_uri TEXT,
            data_insercao TIMESTAMPTZ DEFAULT now()
        );
    ALTER TABLE IF EXISTS  "{schema}".lojaintegrada_categories
        OWNER to {user};	


    CREATE TABLE IF NOT EXISTS  "{schema}".lojaintegrada_list_orders (
        id BIGINT PRIMARY KEY,
        order_number INTEGER,
        customer_uri TEXT,
        
        status_id INTEGER,
        status_code TEXT,
        status_name TEXT,
        status_final BOOLEAN,
        status_approved BOOLEAN,
        status_canceled BOOLEAN,
        status_notify_customer BOOLEAN,
        status_uri TEXT,
        
        subtotal_value NUMERIC,
        shipping_value NUMERIC,
        discount_value NUMERIC,
        total_value NUMERIC,
        real_weight NUMERIC,
        
        external_id TEXT,
        anymarket_id TEXT,
        utm_campaign TEXT,
        integration_data JSONB,
        
        resource_uri TEXT,
        
        created_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ,
        expiration_date TIMESTAMPTZ,
        
        data_insercao TIMESTAMPTZ DEFAULT now()
    );

    ALTER TABLE IF EXISTS  "{schema}".lojaintegrada_list_orders
        OWNER to {user};


    CREATE TABLE IF NOT EXISTS  "{schema}".lojaintegrada_orders (
        id BIGINT PRIMARY KEY,
        order_number INTEGER,
        resource_uri TEXT,

        created_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ,
        expiration_date TIMESTAMPTZ,

        subtotal_value NUMERIC,
        shipping_value NUMERIC,
        discount_value NUMERIC,
        total_value NUMERIC,
        real_weight NUMERIC,

        external_id TEXT,
        anymarket_id TEXT,
        utm_campaign TEXT,
        integration_data JSONB,

        status_id INTEGER,
        status_code TEXT,
        status_name TEXT,
        status_final BOOLEAN,
        status_approved BOOLEAN,
        status_canceled BOOLEAN,
        status_notify_customer BOOLEAN,
        status_uri TEXT,

        client_id BIGINT,
        client_name TEXT,
        client_email TEXT,
        client_cpf TEXT,
        client_birthdate DATE,
        client_gender TEXT,
        client_mobile TEXT,
        client_uri TEXT,

        delivery_zipcode TEXT,
        delivery_city TEXT,
        delivery_state TEXT,
        delivery_address TEXT,
        delivery_number TEXT,
        delivery_complement TEXT,
        delivery_neighborhood TEXT,
        delivery_country TEXT,

        shipments JSONB,
        items JSONB,
        payments JSONB,

        data_insercao TIMESTAMPTZ DEFAULT now()
    );
    
    ALTER TABLE IF EXISTS  "{schema}".lojaintegrada_orders
        OWNER to {user};

    CREATE TABLE IF NOT EXISTS  "{schema}".lojaintegrada_shipments (
        id BIGINT PRIMARY KEY,
        order_uri TEXT,
        object TEXT,
        delivery_time INTEGER,
        shipping_price NUMERIC,

        carrier_id INTEGER,
        carrier_code TEXT,
        carrier_name TEXT,
        carrier_type TEXT,

        created_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ,
        
        data_insercao TIMESTAMPTZ DEFAULT now()
    );  
    
    ALTER TABLE IF EXISTS  "{schema}".lojaintegrada_shipments
        OWNER to {user};  

        
     CREATE TABLE IF NOT EXISTS  "{schema}".lojaintegrada_orders_items (
        id BIGINT PRIMARY KEY,
        order_uri TEXT,
        product_uri TEXT,
        parent_product_uri TEXT,

        sku TEXT,
        name TEXT,
        type TEXT,
        ncm TEXT,
        line INTEGER,
        availability INTEGER,

        height INTEGER,
        width INTEGER,
        depth INTEGER,
        weight NUMERIC,

        quantity NUMERIC,
        full_price NUMERIC,
        cost_price NUMERIC,
        sale_price NUMERIC,
        promotional_price NUMERIC,
        subtotal_price NUMERIC,

        variation JSONB,

        data_insercao TIMESTAMPTZ DEFAULT now()
    );

      ALTER TABLE IF EXISTS  "{schema}".lojaintegrada_orders_items
        OWNER to {user};  


    CREATE TABLE IF NOT EXISTS  "{schema}".lojaintegrada_order_payments (
        id BIGINT PRIMARY KEY,
        order_id bigint not null ,
        amount NUMERIC,
        amount_paid NUMERIC,
        payment_type TEXT,
        transaction_id TEXT,

        authorization_code TEXT,
        gateway_message TEXT,
        gateway_return_code TEXT,
        identifier_id TEXT,

        bank TEXT,
        brand TEXT,
        pix_code TEXT,
        pix_qrcode TEXT,
        boleto_url TEXT,

        installments_number INTEGER,
        installment_value NUMERIC,

        payment_method_id INTEGER,
        payment_method_name TEXT,
        payment_method_code TEXT,
        payment_method_uri TEXT,
        payment_method_logo TEXT,
        payment_method_active BOOLEAN,
        payment_method_available BOOLEAN,

        data_insercao TIMESTAMPTZ DEFAULT now()
    );
    
    ALTER TABLE IF EXISTS  "{schema}".lojaintegrada_order_payments
        OWNER to {user};  

    """

    return scripts



def moovinsqlscripts(schema, user):
    scripts = f"""

    CREATE SCHEMA IF NOT EXISTS "{schema}";
    
    CREATE TABLE IF NOT EXISTS "{schema}".moovin_list_products (
            id TEXT ,
            title TEXT,
            description TEXT,
            video TEXT,
            active BOOLEAN,
            external_link TEXT,
            category_id TEXT,
            category_label TEXT,
            brand_id TEXT,
            brand_label TEXT,
            specifications JSONB,
            variation_attributes JSONB,
            additional_categories JSONB,
            images JSONB,
            groups JSONB,
            variations JSONB,
            hotsites JSONB,
            metadata JSONB,
            created_at TIMESTAMPTZ,
            updated_at TIMESTAMPTZ,
            data_insercao timestamptz DEFAULT now() NULL,
            CONSTRAINT moovin_list_products_pkey PRIMARY KEY (id)
        );

    ALTER TABLE IF EXISTS  "{schema}".moovin_list_products
    OWNER to {user};	

    CREATE TABLE IF NOT EXISTS "{schema}".moovin_categories (
              id TEXT ,
            name TEXT,
            description TEXT,
            seo_uri TEXT,
            banner_desktop TEXT,
            banner_mobile TEXT,
            banner_link TEXT,
            parent_category TEXT,
            children JSONB,
            data_insercao timestamptz DEFAULT now() NULL,
            CONSTRAINT moovin_categories_pkey PRIMARY KEY (id)
        );

    ALTER TABLE IF EXISTS  "{schema}".moovin_categories
    OWNER to {user};	


    CREATE TABLE IF NOT EXISTS "{schema}".moovin_orders (
            id TEXT ,
            order_number INTEGER,

            created_at TIMESTAMPTZ,
            updated_at TIMESTAMPTZ,
            approved_at TIMESTAMPTZ,
            canceled_at TIMESTAMPTZ,

            subtotal_value NUMERIC(10,2),
            shipping_value NUMERIC(10,2),
            discount_value NUMERIC(10,2),
            total_value NUMERIC(10,2),

            external_id TEXT,
            observation TEXT,
            channel TEXT,
            channel_identifier TEXT,

            status TEXT,

            client_id TEXT,
            client_name TEXT,
            client_email TEXT,
            client_document TEXT,
            client_phone TEXT,
            client_type TEXT,

            billing_zipcode TEXT,
            billing_city TEXT,
            billing_state TEXT,
            billing_address TEXT,
            billing_number TEXT,
            billing_complement TEXT,
            billing_neighborhood TEXT,

            delivery_zipcode TEXT,
            delivery_city TEXT,
            delivery_state TEXT,
            delivery_address TEXT,
            delivery_number TEXT,
            delivery_complement TEXT,
            delivery_neighborhood TEXT,

            metadata_ip TEXT,
            metadata_device_type TEXT,
            metadata_browser TEXT,
            metadata_os TEXT,
            metadata_os_version TEXT,
            metadata_entry_url TEXT,

            shippings JSONB,
            payments JSONB,
            data_insercao timestamptz DEFAULT now() NULL,
            CONSTRAINT mmoovin_orders_pkey PRIMARY KEY (id)
        );

    ALTER TABLE IF EXISTS  "{schema}".moovin_orders
    OWNER to {user};	



    CREATE TABLE IF NOT EXISTS "{schema}".moovin_order_shippings (
            id TEXT ,
            order_number bigint, 
            post_date TIMESTAMPTZ,
            delivery_date TIMESTAMPTZ,
            shipping_price NUMERIC(10,2),
            status TEXT,
            processed BOOLEAN,

            quote_id TEXT,

            carrier_id TEXT,
            carrier_code TEXT,
            carrier_name TEXT,
            carrier_type TEXT,
            carrier_method TEXT,
            delivery_time INTEGER,

            tracking_code TEXT,
            tracking_link TEXT,

            items JSONB,
            invoices JSONB,

            updated_at TIMESTAMPTZ,
            data_insercao timestamptz DEFAULT now() NULL,
            CONSTRAINT moovin_order_shippings_pkey PRIMARY KEY (id)
        );

    ALTER TABLE IF EXISTS  "{schema}".moovin_order_shippings
    OWNER to {user};	


    CREATE TABLE IF NOT EXISTS "{schema}".moovin_order_payments (
            id TEXT ,
            order_number bigint,
            payment_type TEXT,
            credit_card_brand TEXT,
            payment_gateway TEXT,

            boleto_barcode TEXT,
            boleto_expiration TIMESTAMPTZ,
            boleto_url TEXT,

            amount NUMERIC(10,2),
            amount_paid NUMERIC(10,2),

            installments_number INTEGER,
            data_insercao timestamptz DEFAULT now() NULL,
            CONSTRAINT moovin_order_payments_pkey PRIMARY KEY (id)
        );

    ALTER TABLE IF EXISTS  "{schema}".moovin_order_payments
    OWNER to {user};	



    CREATE TABLE IF NOT EXISTS "{schema}".moovin_shipping_items (
            id TEXT ,
            order_number INTEGER,

            description TEXT,
            product_id TEXT,
            gtin TEXT,
            variation_sku TEXT,
            image TEXT,

            specifications JSONB,
            warehouses JSONB,

            quantity INTEGER,
            price NUMERIC(10,2),
            price_paid NUMERIC(10,2),
            data_insercao timestamptz DEFAULT now() NULL,
            constraint moovin_shipping_items_pkey PRIMARY KEY (id)
        );

    ALTER TABLE IF EXISTS  "{schema}".moovin_shipping_items
    OWNER to {user};	


    """
    return scripts



def nuvemsqlscripts(schema, user):
    scripts = f"""

    CREATE SCHEMA IF NOT EXISTS "{schema}";
    
    CREATE TABLE IF NOT EXISTS "{schema}".nuvem_orders (
    order_id              bigint     ,
    store_id              int,
    created_at            timestamptz,
    updated_at            timestamptz,
    completed_at          timestamptz,
    next_action           text,
    status                text,
    currency              char(3),
    subtotal_value        numeric(12,2),
    discount_value        numeric(12,2),
    total_value           numeric(12,2),
    shipping_cost_owner   numeric(12,2),
    shipping_cost_customer numeric(12,2),
    shipping_tracking_number text,
    gateway               text,
    gateway_name          text,
    payment_count         int,
    customer_id           bigint,
    customer_name         text,
    customer_email        text,
    customer_document     text,
    customer_phone        text,
    billing_zipcode       text,
    billing_city          text,
    billing_state         text,
    billing_address       text,
    billing_number        text,
    billing_locality      text,
    items              		jsonb,
    method_payment           text,   
    credit_card_brand_payment   text,
    data_insercao         timestamptz DEFAULT now() null,  
    CONSTRAINT nuvem_orders_pkey PRIMARY KEY (order_id)
);

    ALTER TABLE IF EXISTS  "{schema}".nuvem_orders
    OWNER to {user};	

    
    CREATE TABLE "{schema}".nuvem_order_items (
    id                   bigint,
    order_id             bigint ,
    product_id           bigint,
    variant_id           bigint,
    sku                  text,
    barcode              text,
    name                 text,
    name_without_variants text,
    price                numeric(12,2),
    cost                 numeric(12,2),
    quantity             int,
    weight               numeric(12,3),
    width                numeric(12,3),
    height               numeric(12,3),
    depth                numeric(12,3),
    free_shipping        boolean,
    data_insercao        timestamptz DEFAULT now(),
    CONSTRAINT constraint_nuvem_order_items_unique UNIQUE (id, order_id)
	
);

    ALTER TABLE IF EXISTS  "{schema}".nuvem_order_items
    OWNER to {user};	

    
    CREATE TABLE "{schema}".nuvem_products (
    id bigint PRIMARY KEY,
    name text,
    handle text,
    published boolean,
    free_shipping boolean,
    requires_shipping boolean,
    created_at timestamptz,
    updated_at timestamptz,
    tags text,
    categories jsonb,               -- ← array completo vindo da API
    category_main_id bigint,        -- ← ID da primeira categoria principal
    category_main_name text,        -- ← Nome da categoria principal
    data_insercao timestamptz DEFAULT now()
);

    ALTER TABLE IF EXISTS  "{schema}".nuvem_products
    OWNER to {user};	

    
    CREATE TABLE "{schema}".nuvem_categories (
    id bigint PRIMARY KEY,
    name text,
    handle text,
    subcategories jsonb,
    google_shopping_category text,
    created_at timestamptz,
    updated_at timestamptz,
    data_insercao timestamptz DEFAULT now()
);

    ALTER TABLE IF EXISTS  "{schema}".nuvem_categories
    OWNER to {user};	


    """
    return scripts

# if __name__ == "__main__":
#     with open("Output.txt", "w") as text_file:
#         text_file.write(vtexsqlscripts("6d41d249-d875-41ef-800e-eb0941f6d86f"))
#         print(vtexsqlscripts("6d41d249-d875-41ef-800e-eb0941f6d86f"))
