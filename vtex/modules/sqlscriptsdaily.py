def vtexsqlscriptsorderslistupdate(schema):
    scripts = f"""
                
        DROP TABLE IF exists tmp_orders_list_daily_old;

        CREATE TEMPORARY TABLE tmp_orders_list_daily_old 
        as
            SELECT 
            sequence_id, orderid, creationdate, lastchange
            FROM "{schema}".orders_list
            where creationdate >= (CURRENT_DATE  - INTERVAL '90 days');

        DROP TABLE IF exists tmp_orders_list_daily_new;


        CREATE TEMPORARY TABLE tmp_orders_list_daily_new 
        as
        select 
		orderid
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
        ,data_insercao

        from "{schema}".orders_list_daily ora

        left join tmp_orders_list_daily_old tmp  on 
        tmp.orderid = orderid 
        and 
        tmp.lastchange = lastchange

        where 
        tmp.orderid is null 
        and 
        creationdate>= (select min(creationdate) from tmp_orders_list_daily_old );







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


    """
    print(scripts)
    return scripts


# if __name__ == "__main__":
#     with open("Output.txt", "w") as text_file:
#         text_file.write(vtexsqlscriptsorderslistupdate("6d41d249-d875-41ef-800e-eb0941f6d86f"))
#         print(vtexsqlscriptsorderslistupdate("6d41d249-d875-41ef-800e-eb0941f6d86f"))
