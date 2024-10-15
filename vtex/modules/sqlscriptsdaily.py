def vtexsqlscriptsorderslistupdate(schema):
    scripts = f"""
        		DROP TABLE IF EXISTS tmp_orders_list_daily_old;
        CREATE TEMPORARY TABLE tmp_orders_list_daily_old AS
        SELECT
            sequence_id, orderid, creationdate, lastchange,status
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
            tmp.lastchange = ora.lastchange and 
            tmp.status= ora.status
            
        WHERE
            tmp.orderid IS NULL AND
            ora.creationdate >= (SELECT MIN(creationdate) FROM tmp_orders_list_daily_old);

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
        from tmp_orders_list_daily_new
        ON CONFLICT (orderid)
  		DO UPDATE SET 
			creationdate	=	EXCLUDED.creationdate
		,	clientname	=	EXCLUDED.clientname
		,	items	=	EXCLUDED.items
		,	totalvalue	=	EXCLUDED.totalvalue
		,	paymentnames	=	EXCLUDED.paymentnames
		,	status	=	EXCLUDED.status
		,	statusdescription	=	EXCLUDED.statusdescription
		,	marketplaceorderid	=	EXCLUDED.marketplaceorderid
		,	"sequence"	=	EXCLUDED."sequence"
		,	saleschannel	=	EXCLUDED.saleschannel
		,	affiliateid	=	EXCLUDED.affiliateid
		,	origin	=	EXCLUDED.origin
		,	workflowinerrorstate	=	EXCLUDED.workflowinerrorstate
		,	workflowinretry	=	EXCLUDED.workflowinretry
		,	lastmessageunread	=	EXCLUDED.lastmessageunread
		,	shippingestimateddate	=	EXCLUDED.shippingestimateddate
		,	shippingestimateddatemax	=	EXCLUDED.shippingestimateddatemax
		,	shippingestimateddatemin	=	EXCLUDED.shippingestimateddatemin
		,	orderiscomplete	=	EXCLUDED.orderiscomplete
		,	listid	=	EXCLUDED.listid
		,	listtype	=	EXCLUDED.listtype
		,	authorizeddate	=	EXCLUDED.authorizeddate
		,	callcenteroperatorname	=	EXCLUDED.callcenteroperatorname
		,	totalitems	=	EXCLUDED.totalitems
		,	currencycode	=	EXCLUDED.currencycode
		,	hostname	=	EXCLUDED.hostname
		,	invoiceoutput	=	EXCLUDED.invoiceoutput
		,	invoiceinput	=	EXCLUDED.invoiceinput
		,	lastchange	=	EXCLUDED.lastchange
		,	isalldelivered	=	EXCLUDED.isalldelivered
		,	isanydelivered	=	EXCLUDED.isanydelivered
		,	giftcardproviders	=	EXCLUDED.giftcardproviders
		,	orderformid	=	EXCLUDED.orderformid
		,	paymentapproveddate	=	EXCLUDED.paymentapproveddate
		,	readyforhandlingdate	=	EXCLUDED.readyforhandlingdate
		,	deliverydates	=	EXCLUDED.deliverydates
		,	data_insercao	=	EXCLUDED.data_insercao
        ,   is_change = TRUE    




    """
    # print(scripts)
    return scripts

# if __name__ == "__main__":
#     with open("Output.txt", "w") as text_file:
#         text_file.write(vtexsqlscriptsorderslistupdate("6d41d249-d875-41ef-800e-eb0941f6d86f"))
#         print(vtexsqlscriptsorderslistupdate("6d41d249-d875-41ef-800e-eb0941f6d86f"))