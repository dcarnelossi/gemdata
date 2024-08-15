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
        cancellationrequests character varying COLLATE pg_catalog."default",
        CONSTRAINT constraint_orders_orderid UNIQUE (orderid)
    )

    TABLESPACE pg_default;

    ALTER TABLE IF EXISTS "{schema}".orders
        OWNER to {user};


    CREATE INDEX IF NOT EXISTS "idx_orders_orderid"
        ON "{schema}".orders USING btree
        (orderid COLLATE pg_catalog."default" ASC NULLS LAST)
        TABLESPACE pg_default;



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


    CREATE VIEW IF NOT EXISTS "{schema}".counts_view AS
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


if __name__ == "__main__":
    with open("Output.txt", "w") as text_file:
        text_file.write(vtexsqlscripts("6d41d249-d875-41ef-800e-eb0941f6d86f"))
        print(vtexsqlscripts("6d41d249-d875-41ef-800e-eb0941f6d86f"))
