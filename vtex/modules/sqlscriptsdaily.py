def vtexsqlscriptsorderslistupdate(schema):
    scripts = f"""
        DROP TABLE IF EXISTS tmp_orders_list_daily_old;
        CREATE TEMPORARY TABLE tmp_orders_list_daily_old AS
        SELECT
            sequence_id, orderid, creationdate, lastchange
        FROM "{schema}".orders_list
        WHERE creationdate >= (CURRENT_DATE - INTERVAL '90 days');

        DROP TABLE IF EXISTS tmp_orders_list_daily_new;
        CREATE TEMPORARY TABLE tmp_orders_list_daily_new AS
        SELECT
            ora.orderid,
            ora.creationdate,
            ora.clientname,
            ora.items,
            ora.totalvalue,
            ora.paymentnames,
            ora.status,
            ora.statusdescription,
            ora.marketplaceorderid,
            ora."sequence",
            ora.saleschannel,
            ora.affiliateid,
            ora.origin,
            ora.workflowinerrorstate,
            ora.workflowinretry,
            ora.lastmessageunread,
            ora.shippingestimateddate,
            ora.shippingestimateddatemax,
            ora.shippingestimateddatemin,
            ora.orderiscomplete,
            ora.listid,
            ora.listtype,
            ora.authorizeddate,
            ora.callcenteroperatorname,
            ora.totalitems,
            ora.currencycode,
            ora.hostname,
            ora.invoiceoutput,
            ora.invoiceinput,
            ora.lastchange,
            ora.isalldelivered,
            ora.isanydelivered,
            ora.giftcardproviders,
            ora.orderformid,
            ora.paymentapproveddate,
            ora.readyforhandlingdate,
            ora.deliverydates,
            ora.data_insercao
        FROM "{schema}".orders_list_daily ora
        LEFT JOIN tmp_orders_list_daily_old tmp ON
            tmp.orderid = ora.orderid AND
            tmp.lastchange = ora.lastchange
        WHERE
            tmp.orderid IS NULL AND
            ora.creationdate >= (SELECT MIN(creationdate) FROM tmp_orders_list_daily_old);

            
        truncate table "{schema}".orders_list_daily;

        DELETE FROM "{schema}".orders_list
        WHERE orderid IN (SELECT orderid FROM tmp_orders_list_daily_new);

        DELETE FROM "{schema}".orders
        WHERE orderid IN (SELECT orderid FROM tmp_orders_list_daily_new);

        DELETE FROM "{schema}".orders_items
        WHERE orderid IN (SELECT orderid FROM tmp_orders_list_daily_new);

        left join tmp_orders_list_daily_old tmp  on 
        tmp.orderid = ora.orderid 
        and 
        tmp.lastchange = ora.lastchange

        where 
        tmp.orderid is null 
        and 
        ora.creationdate>= (select min(creationdate) from tmp_orders_list_daily_old );


        delete from  "{schema}".orders_list 
        where orderid in (select orderid from tmp_orders_list_daily_new);


        delete from  "{schema}".orders
        where orderid in (select orderid from tmp_orders_list_daily_new);

        delete from "{schema}".orders_items
        where orderid in (select orderid from tmp_orders_list_daily_new);


        delete from  "{schema}".orders_shippingdata
        where orderid in (select orderid from tmp_orders_list_daily_new);


        delete from "{schema}".orders_totals
        where orderid in (select orderid from tmp_orders_list_daily_new);


        delete from "{schema}".client_profile
        where orderid in (select orderid from tmp_orders_list_daily_new);

        insert into "{schema}".orders_list (orderid
        ,creationdate
        ,clientname
        ,items
        ,totalvalue
        ,paymentnames
        ,status
        ,statusdescription
        ,marketplaceorderid
        ,"sequence"
        ,saleschannel
        ,affiliateid
        ,origin
        ,workflowinerrorstate
        ,workflowinretry
        ,lastmessageunread
        ,shippingestimateddate
        ,shippingestimateddatemax
        ,shippingestimateddatemin
        ,orderiscomplete
        ,listid
        ,listtype
        ,authorizeddate
        ,callcenteroperatorname
        ,totalitems
        ,currencycode
        ,hostname
        ,invoiceoutput
        ,invoiceinput
        ,lastchange
        ,isalldelivered
        ,isanydelivered
        ,giftcardproviders
        ,orderformid
        ,paymentapproveddate
        ,readyforhandlingdate
        ,deliverydates
        ,data_insercao)
        select
        *
        from tmp_orders_list_daily_new;

        DELETE FROM "{schema}".orders_totals
        WHERE orderid IN (SELECT orderid FROM tmp_orders_list_daily_new);

        DELETE FROM "{schema}".client_profile
        WHERE orderid IN (SELECT orderid FROM tmp_orders_list_daily_new);

        INSERT INTO "{schema}".orders_list (
            orderid,
            creationdate,
            clientname,
            items,
            totalvalue,
            paymentnames,
            status,
            statusdescription,
            marketplaceorderid,
            "sequence",
            saleschannel,
            affiliateid,
            origin,
            workflowinerrorstate,
            workflowinretry,
            lastmessageunread,
            shippingestimateddate,
            shippingestimateddatemax,
            shippingestimateddatemin,
            orderiscomplete,
            listid,
            listtype,
            authorizeddate,
            callcenteroperatorname,
            totalitems,
            currencycode,
            hostname,
            invoiceoutput,
            invoiceinput,
            lastchange,
            isalldelivered,
            isanydelivered,
            giftcardproviders,
            orderformid,
            paymentapproveddate,
            readyforhandlingdate,
            deliverydates,
            data_insercao
        )
        SELECT *
        FROM tmp_orders_list_daily_new;
    """
    print(scripts)
    return scripts

# if __name__ == "__main__":
#     with open("Output.txt", "w") as text_file:
#         text_file.write(vtexsqlscriptsorderslistupdate("6d41d249-d875-41ef-800e-eb0941f6d86f"))
#         print(vtexsqlscriptsorderslistupdate("6d41d249-d875-41ef-800e-eb0941f6d86f"))