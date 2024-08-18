from capitaliq.cfg import SERVER_TIMEZONE,DBINFO
import pandas as pd
from datetime import datetime, timedelta
import json
import psycopg2


def read_sql_to_df(sql, db, cursor):
    """Internal: execute sql and get dataframe as a return 
    
    Args:
        sql (str): sql to be executed 
        db (psycopg2.connect): connect to ciq target database 
        cursor (database connection.cursor): 
    
    Returns:
        pd.DataFrame: contains result for executed sql 
    """
    try:
        cursor.execute(sql)
        data = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        return pd.DataFrame(data,columns = columns)
    except (Exception, psycopg2.DatabaseError) as error:
        db.rollback()
        print("Error: %s" % error)


def get_connection(dbInfo):
    """get the db connection using info in dbInfo json file
    
    Args:
        dbInfo (.json): user-pw for ciq target database 
    
    Returns:
        psycopg2.connect
    """
    with open(dbInfo,'r') as f:
        u = json.load(f)

    db = psycopg2.connect(
        host=u['host'],
        database=u["database"],
        user=u["user"],
        password=u["pwd"],
        port=u["port"])
    return db


def get_traded_isin_company(date, countrycode = 213, currencyid = 160, mktcap_thres = 250, adv_thres = 1e6, connection = None):
    '''
    get the idmaps from company id to corresponding isin
    :param date: universe as of the date (incl.)
    :return:
    Extract tradeable companies and its ISINs:
        given that the companies meets MKTCAP_THRES --
                most recent market cap if exists within last 10 days
            and (ATV)average trading volume ADV_THRES --
                ATV is calculated using mean(last 360days)
    
    Args:
        date (str): '2020-03-03' (incl.)
        connection (None, optional): connection = get_connection(DBINFO)
    
    Returns:
        pd.DataFrame: sample:
        df:               isin  securityid      securityname  tradingitemid  companyid                      companyname     marketcap                 volume
        0     US0003602069     2585892      Common Stock        2585893     320500                       AAON, Inc.   2926.540470   7015116.145080645161
        1     US0003611052     2585894      Common Stock        2585895     168154                        AAR Corp.   1254.929933   9664062.786411290323
        2     US0009571003     2586015      Common Stock        2586016     250079      ABM Industries Incorporated   2265.596955  12838018.320443548387
        3     US0247531058     2586015      Common Stock        2586016     250079      ABM Industries Incorporated   2265.596955  12838018.320443548387
        4     US0258701061     2586085      Common Stock        2586086     250178               Aflac Incorporated  31306.821931     162019261.84407258
    '''

    datestr = (
        pd.to_datetime(date)
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
    )

    mc_delay_date = ((pd.to_datetime(date) - timedelta(days = 10))
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
            )
 
    volstart = ((pd.to_datetime(date) - timedelta(days = 360))
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
            )

    sql = f"""
	DROP TABLE IF EXISTS preProcessed;

	CREATE TEMP TABLE preProcessed AS
	SELECT 
	idm.identifiervalue as isin
	,idm.activeflag as isinflag
	, s.securityid
	, s.securityname
	, ti.tradingitemID
	, cpny.companyID
	, cpny.companyname
	FROM
	ciqsecurityidentifier AS idm
	INNER JOIN ciqSecurity AS s ON s.securityId=idm.securityId
	INNER JOIN ciqCompany AS cpny on cpny.companyID=s.companyID
	INNER JOIN ciqTradingItem AS ti ON ti.securityId=s.securityId
	INNER JOIN ciqExchange AS ec ON ti.exchangeid = ec.exchangeid

	WHERE 
	s.securitySubTypeId=1 AND s.primaryflag=1
	AND
	ti.primaryflag=1 AND ti.currencyid={currencyid}
	AND
	ec.countryid={countrycode};

	-- EXPLAIN ANALYZE
	SELECT 
	preProcessed.*,
	mcm.marketcap,
	adv.volume

	FROM 
	preProcessed 

	INNER JOIN 
	(
		SELECT 
			mc.companyID, 
			mc.marketcap
	    FROM ciqMarketCap AS mc
	    INNER JOIN 
		(
		SELECT companyID, MAX(pricingdate) AS MaxDate
		FROM ciqMarketCap
			WHERE 
				pricingdate BETWEEN '{mc_delay_date}' AND '{datestr}'
			AND
				companyID IN (SELECT companyID FROM preProcessed)
		GROUP BY companyID
	    ) AS tm 
		ON mc.companyID = tm.companyID AND mc.pricingdate = tm.MaxDate
		WHERE mc.marketcap >= {mktcap_thres} 
	) AS mcm 	
	ON mcm.companyID=preProcessed.companyID

	INNER JOIN 
	(
		SELECT 
			tradingItemId, 
			AVG(volume * priceClose) AS volume
		FROM targetskma.ciqPriceEquity
		WHERE 
			pricingDate BETWEEN '{volstart}' AND '{datestr}'
		AND 
			tradingItemId IN (SELECT tradingitemID FROM preProcessed)
		GROUP BY tradingItemId
	) AS adv 
	ON preProcessed.tradingItemId=adv.tradingItemId
	AND adv.volume >= {adv_thres}
    """

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
    
    return read_sql_to_df(sql, connection, cursor)

# function alias
get_tradable_company = get_traded_isin_company


def get_company_transcripts(start, end, ls_ids, connection = None):
    """
    Get the transcripts of given companies id from start date to end date
    Args:
        start (str): start date (incl.)   '2020-03-03'
        end (str): end date (incl.)    '2020-03-04'
        ls_ids (list): list of companyid   [11686323, ]
        connection (None, optional): Description
    
    Returns:
        sample ouput: 
            df:       transcriptid transcriptcreationdateutc  companyid                                      componenttext transcriptcomponenttypename  ...                                              title  speakertypename   personid      porg  delayreasontypeid
                0          1927158       2020-03-03 00:04:18   11686323  It's Nick on for Jim this afternoon. Just cong...                    Question  ...                       Director & Associate Analyst         Analysts   27864075   9589274               None
                1          1927158       2020-03-03 00:04:18   11686323  Okay. And then the last question is regarding ...                    Question  ...  MD of Equity Research & Senior Healthcare Analyst         Analysts   33941017  25231031               None
                2          1927158       2020-03-03 00:04:18   11686323  Sure. We've got a slide actually that we -- th...                      Answer  ...                              Former CEO & Director       Executives  297984959  11686323               None
                3          1927158       2020-03-03 00:04:18   11686323  Yes. No, I think as the data come in, especial...                      Answer  ...                              Former CEO & Director       Executives  297984959  11686323               None
                4          1927158       2020-03-03 00:04:18   11686323  Just on the NCI program, as you're learning mo...                    Question  ...  MD of Equity Research & Senior Healthcare Analyst         Analysts   33941017  25231031               None

    """
    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
    
    startdatestr = (
        pd.to_datetime(start)
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
    )
    enddatestr = (
        pd.to_datetime(end)
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
    )

    sql = """
    CREATE TEMP TABLE temp_universe
    (
        companyid int
    )
    ON COMMIT DELETE ROWS;
    BEGIN TRANSACTION;
    """
    cursor.execute(sql)

    sql = """SELECT * FROM information_schema.tables"""
    cursor.execute(sql)
    data = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    schema = pd.DataFrame(data,columns = columns).table_schema.unique()
    for i in schema:
        if 'pg_temp' in i:
            tschema = i

    company_list = ""
    for cid in ls_ids:
        company_list += "INSERT INTO %s.temp_universe VALUES (%s);\n"%(tschema,cid)
        
    sql = f"""
            {company_list}
            SELECT t.transcriptId, t.transcriptCreationDateUTC, 
            comp.companyId,
            CAST(tc.componentText AS TEXT) AS componenttext,tcty.transcriptComponentTypeName, 
            tc.transcriptComponentId, tc.componentOrder, tc.transcriptComponentTypeId,
            t.transcriptCollectionTypeId,
            t.keyDevId,
            e.mostImportantDateUTC as EarningsDateUTC, e.announcedDateUTC, 
            eb.fiscalyear, eb.fiscalquarter,
            pb.title, pt.speakerTypeName, pb.personid, pb.companyid as pOrg,
            dr.delayReasonTypeId
            FROM targetskma.ciqTranscript t
            JOIN targetskma.ciqTranscriptComponent tc ON tc.transcriptId = t.transcriptId
            JOIN targetskma.ciqTranscriptComponentType tcty ON
            tcty.transcriptComponentTypeId = tc.transcriptComponentTypeId
            JOIN targetskma.ciqTranscriptPerson p ON p.transcriptpersonid = tc.transcriptpersonid
            JOIN targetskma.ciqTranscriptSpeakerType pt ON p.speakerTypeId = pt.speakerTypeId
            JOIN targetskma.ciqProfessional pb on pb.proId= p.proId
            JOIN targetskma.ciqEvent e ON e.keyDevId = t.keyDevId
            JOIN targetskma.ciqEventToObjectToEventType ete ON ete.keyDevId = t.keyDevId
            JOIN targetskma.ciqCompany comp ON comp.companyId = ete.objectId
            JOIN targetskma.ciqEventType et ON et.keyDevEventTypeId = ete.keyDevEventTypeId
            JOIN targetskma.ciqeventcallbasicinfo eb on eb.keyDevId = t.keyDevId
            LEFT JOIN targetskma.ciqTranscriptDelayReason dr on dr.keyDevId = t.keyDevId
            WHERE et.keyDevEventTypeId='48' --Earnings Calls
            AND t.transcriptId in (SELECT DISTINCT a.transcriptId FROM targetskma.ciqTranscript a,
            (
            SELECT keyDevId,Min(transcriptCreationDateUTC) as minTm FROM targetskma.ciqTranscript
            WHERE transcriptCollectionTypeId!=7
            GROUP BY keyDevId
            ) mtm
            WHERE a.keyDevId = mtm.keyDevId AND a.transcriptCreationDateUTC = mtm.minTm)
            AND comp.companyid in (SELECT companyid FROM {tschema}.temp_universe)
            AND t.transcriptCreationDateUTC >= '{startdatestr}'
            AND t.transcriptCreationDateUTC <= '{enddatestr}'           
            ORDER BY t.transcriptCreationDateUTC asc;
            """    
    
    return read_sql_to_df(sql, connection, cursor)

# function alias
get_transcripts = get_company_transcripts


def get_pricing(start, end, ls_ids, connection = None):
    """
    Get pricing of given companies from start date to end date
    
    Args:
        start (str): start date (incl.)   '2020-03-03'
        end (str): end date (incl.)    '2020-03-04'
        ls_ids (list): list of companyid   [11686323, ]
        connection (None, optional): Description
    
    Returns:
        sample ouput: 
        df:   companyid pricingdate  priceclose   priceopen   pricehigh    pricelow     volume           divadjprice  divadjfactor
        0         25142  2020-03-03  106.780000  109.030000  110.200000  104.850000   703117.0  106.7800000000000000  1.0000000000
        1        343596  2020-03-03   59.620000   59.790000   61.570000   58.510000   536387.0   59.5637918379680000  0.9990572264
        2        252978  2020-03-03  117.960000  119.190000  122.330000  116.795000   811069.0  115.8757946864520000  0.9823312537
        3        764293  2020-03-03    3.110000    3.450000    3.530000    3.095000  2636682.0    3.1100000000000000  1.0000000000
        4         25026  2020-03-03   46.940000   46.670000   48.220000   46.170200   683089.0             46.940000             1

    """

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()

    startdatestr = (
        pd.to_datetime(start)
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
    )
    enddatestr = (
        pd.to_datetime(end)
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
    )

    sql = """
    CREATE TEMP TABLE temp_universe
    (
        companyid int
    )
    ON COMMIT DELETE ROWS;
    BEGIN TRANSACTION;
    """
    cursor.execute(sql)

    sql = """SELECT * FROM information_schema.tables"""
    cursor.execute(sql)
    data = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    schema = pd.DataFrame(data,columns = columns).table_schema.unique()
    for i in schema:
        if 'pg_temp' in i:
            tschema = i

    company_list = ""
    for cid in ls_ids:
        company_list += "INSERT INTO %s.temp_universe VALUES (%s);\n"%(tschema,cid)

    sql = f"""
    {company_list}
    SELECT c.companyid
    ,pe.pricingDate
    ,pe.priceClose
    ,pe.priceOpen
    ,pe.priceHigh
    ,pe.priceLow
    ,pe.volume
    ,(pe.priceClose*COALESCE(daf.divAdjFactor,1)) divAdjPrice
    ,COALESCE(daf.divAdjFactor,1) as divAdjFactor
    from targetskma.ciqCompany c
    join targetskma.ciqSecurity s on c.companyid=s.companyid 
    join targetskma.ciqTradingItem ti on ti.securityId=s.securityId
    join targetskma.ciqPriceEquity pe on pe.tradingItemId=ti.tradingItemId
    left join targetskma.ciqPriceEquityDivAdjFactor daf on pe.tradingItemId=daf.tradingItemId
    and daf.fromDate<=pe.pricingDate --Find dividend adjustment factor on pricing date
    and (daf.toDate is null or daf.toDate>=pe.pricingDate)
    WHERE c.companyid in (SELECT companyid FROM {tschema}.temp_universe)
    and s.primaryflag=1
    and ti.primaryflag=1
    and pe.pricingDate>='{startdatestr}'
    and pe.pricingDate<='{enddatestr}'
    ORDER BY pe.pricingDate asc
    """
 
    pr = read_sql_to_df(sql, connection, cursor)   
    
    return pr


def get_PIT_fundamental(ls_ids, date, ls_dataitemid, lookback = 365, connection = None):
    """
    Get point in time shares from CIQ PIT premium financials
    CIQ PIT database has a backfill mechanism causing some instance dates to be much later than filing date for earlier years
    but normally the maximum processing time is less than a month, so assume any lag greater than a month is a backfill data
    
    use the following query to get the dataitem id
    
    select a.dataItemName, a.dataItemId from ciqDataItem a inner join ciqFinCollectionData b
    on a.dataItemId=b.dataItemId where a.dataItemName like '%split%'
    
    :return: dataframe of PIT shares
    
    Args:
        ls_ids (list): list of companyid   [11686323, ]
        date (str): as of date '2020-03-03'
        ls_dataitemid (list): dataitemid in table ciqDataItem
                                1100 is common shares outstanding (adjusted)
                                4379 is normalized basic EPS (comparable to consensus)
                                112987 is book value
                                e.g. [1100, 4379, 112987]
        lookback (int, optional): put a timely check to filter out the data that's too old e.g. 365
        connection (None, optional):
    
    Returns:
        TYPE: dataframe of PIT shares
        sample output:
        ic| df:    companyid              companyname  ...                      description row_n
                0   11686323  ZIOPHARM Oncology, Inc.  ...  Actual data feed insertion date     1

    """

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()

    if isinstance(date, str):
        date = pd.to_datetime(date)

    datestr = date.strftime('%Y-%m-%d')
    # according to SP, the maximum processing time is less than one month, so put OneMonthDelayDatestr for filingdate filter when instance is backfilled
    OneMonthDelayDatestr = (date - timedelta(days=30)).strftime('%Y-%m-%d')
    # put a timely check to filter out the data that's too old
    TimelyDatestr = (date - timedelta(days=lookback)).strftime('%Y-%m-%d')


    sql = """
    CREATE TEMP TABLE temp_universe
    (
        companyid int
    )
    ON COMMIT DELETE ROWS;
    BEGIN TRANSACTION;
    """
    cursor.execute(sql)

    sql = """SELECT * FROM information_schema.tables"""
    cursor.execute(sql)
    data = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    schema = pd.DataFrame(data,columns = columns).table_schema.unique()
    for i in schema:
        if 'pg_temp' in i:
            tschema = i

    sec_list = ""
    for cid in ls_ids:
        sec_list += "INSERT INTO %s.temp_universe VALUES (%s);\n"%(tschema,cid)

    sql = f"""
        {sec_list}
        with PIT as
            (
            select fid.financialInstanceId,fid.instanceDate,fid.instanceDateTypeId, fidt.description
            from targetskma.ciqFinInstanceDate fid
            join targetskma.ciqFinInstanceDateType fidt on fid.instanceDateTypeId = fidt.instanceDateTypeId
            --where fid.InstanceDateTypeId = 1 --Note: You can choose to use only instanceDateTypeId = 1 which indicates the Xpressfeed feed physical file delivery date.
            ),
        fin_hist as
        (
        select c.companyId, c.companyName, fi.financialInstanceId,cast(fi.periodEndDate as date) periodEndDate,fp.fiscalYear, fp.fiscalQuarter, fp.calendarYear,fp.calendarQuarter,rt.restatementTypeName,pt.periodTypeName,
        fi.formType,cast(fi.filingDate as date) filingDate,PIT.instanceDate,di.dataItemName, fd.dataItemValue, 
        PIT.instanceDate - fi.filingDate as lagDays,PIT.instanceDateTypeId,PIT.description, 
        ROW_NUMBER() OVER (
                             PARTITION BY c.companyId, di.dataItemName
                             ORDER BY periodEndDate desc, instanceDate desc
                       ) AS row_n
        from
        PIT
        join targetskma.ciqFinInstance fi on PIT.financialInstanceId = fi.financialInstanceId
        join targetskma.ciqRestatementType rt on fi.restatementTypeId = rt.restatementTypeId
        join targetskma.ciqFinPeriod fp on fi.financialPeriodId = fp.financialPeriodId
        join targetskma.ciqPeriodType pt on fp.periodTypeId = pt.periodTypeId
        join targetskma.ciqCompany c on fp.companyId = c.companyId
        join targetskma.ciqFinInstanceToCollection ic on ic.financialInstanceId = fi.financialInstanceId
        join targetskma.ciqFinCollection fc on fc.financialCollectionId = ic.financialCollectionId
        join targetskma.ciqFinCollectionData fd on fd.financialCollectionId = ic.financialCollectionId
        join targetskma.ciqDataItem di on di.dataItemId = fd.dataItemId
        where c.companyId in (SELECT companyid FROM {tschema}.temp_universe)
        -- c.companyId=112350
        --and fi.formType = '10-Q'
        --and rt.restatementTypeName in ('Press Release', 'Original')
        --and rt.restatementTypeName = 'Original'
        --and pt.periodTypeName = 'Quarterly'
        and pt.periodTypeId in (1, 2, 3, 10, 17) --(1   Annual; 2   Quarterly; 3    YTD; 4  LTM; 10 Semi-Annual; 17 Interim)
        and (fi.filingDate < '{OneMonthDelayDatestr}' or PIT.instanceDate < '{date}')
        and fi.filingDate > '{TimelyDatestr}'
        and di.dataItemId in ({', '.join([str(id) for id in ls_dataitemid])})
        )
        select * from fin_hist where fin_hist.row_n=1
        """
    
    return read_sql_to_df(sql, connection, cursor)         


def get_funds_contain_words(words, connection = None):
    """
    List the funds with name containing words in a given list

    Args:
        words (list): ['vix', 's&p']
        connection (None, optional): 
    
    Returns:
        sample output:
    df:     companyid                                    fundcompanyname  countryid               city  parentcompanyid                                  parentcompanyname
        0    52670059             iPath S&P 500 VIX Mid-Term Futures ETN        212             London         45275521  Barclays Bank PLC, Wealth and Investment Manag...
        1    52670074           iPath S&P 500 VIX Short-Term Futures ETN        212             London         45275521  Barclays Bank PLC, Wealth and Investment Manag...
        2   106585332         BetaPro S&P 500 VIX Short-Term Futures ETF         37            Toronto         28760961            Mirae Asset Global Investments Co., Ltd
        3   118301050   iPath Inverse S&P 500 VIX Short-Term Futures ETN        212             London         45275521  Barclays Bank PLC, Wealth and Investment Manag...
        4   141492325                      iPath S&P 500 Dynamic VIX ETN        212             London         45275521  Barclays Bank PLC, Wealth and Investment Manag...
        5   269118761                  Coba ETN 1x VIXF Daily Long Index         76  Frankfurt Am Main         54496609               Commerzbank AG, Asset Management Arm
        6   269118790                Coba ETN -1x VIXF Daily Short Index         76  Frankfurt Am Main         54496609               Commerzbank AG, Asset Management Arm
        7   269324616                  Coba ETN 2x VIXF Daily Long Index         76  Frankfurt Am Main         54496609               Commerzbank AG, Asset Management Arm
        8   269324702                Coba ETN -2x VIXF Daily Short Index         76  Frankfurt Am Main         54496609               Commerzbank AG, Asset Management Arm
        9   269510789  iPath S&P 500 VIX Mid-Term Futures Exchange Tr...        212             London         45275521  Barclays Bank PLC, Wealth and Investment Manag...
        10  548515655  iPath Series B S&P 500 VIX Short-Term Futures ETN        212             London         45275521  Barclays Bank PLC, Wealth and Investment Manag...
        11  548519012    iPath Series B S&P 500 VIX Mid-Term Futures ETN        212             London         45275521  Barclays Bank PLC, Wealth and Investment Manag...
   
    """

    sql = f"""
            select DISTINCT on (cOwnerFund.companyid) cOwnerFund.companyid, cOwnerFund.companyName as fundCompanyName, cOwnerFund.countryid,cOwnerFund.city,
            up.parentCompanyId as parentCompanyId, cParent.companyName as parentCompanyName
            from targetskma.ciqCompany cOwnerFund
            join targetskma.ciqOwnFundToFundParent fp on cOwnerFund.companyid = fp.fundCompanyId and fp.companyRelTypeId in (17,34)
            left join targetskma.ciqOwnUltimateFundParent up on fp.managerCompanyId = up.parentCompanyId or fp.managerCompanyId = up.childCompanyId
            join targetskma.ciqCompany cParent on up.parentCompanyId = cParent.companyId
            where UPPER(cOwnerFund.companyName) similar to '%({'|'.join(['(%s)'%i.upper() for i in words])})%'
            and fp.endDate is null
            """
    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
                
    return read_sql_to_df(sql, connection, cursor) 




def get_funds_details(fund_ids, connection = None):
    """
    List the characteristics of a fund at a given date (based on most recently available reporting and current prices)
    
    Args:
        fund_ids (list): ciqOwnCompanyInvestStyle.ownerObjectId e.g. [26976968, 43966086, 603285115]
        connection (None, optional): Description
    
    Returns:
        sample output
    df:    ownerobjectid                                        companyname  sizestyleid sizestylename  valgrowstyleid  ... increasedpositions decreasedpositions soldoutpositions  percentofportfolio hasportfolio
        0       26976968  Metropolitan West Funds - Metropolitan West St...            2       Mid cap               3  ...                  0                  0                0              100.00            1
        1       43966086  Victory Variable Insurance Funds - Victory INC...            1     Large cap               2  ...                  0                  0                0              100.00            1
        2      603285115                          Celerity Ci Balanced Fund            1     Large cap               6  ...                  0                  0                0              100.00            1
        

    """


    sql = f"""
            select distinct on (cis.ownerObjectId) cis.ownerObjectId, c.companyName, cis.sizeStyleId, sst.sizeStyleName, cis.valGrowStyleId, vgst.valGrowStyleName,
            cis.portfolioValueScore, cis.portfolioGrowthScore, cis.highYieldFlag, cis.portfolioDivYieldScore
            ,ps.subTypeId
            ,st.subTypeValue
            ,ps.medianPE
            ,ps.medianPB
            ,ps.medianEBITDA
            ,ps.medianTotalRevenue
            ,ps.medianNetIncome
            ,ps.medianMarketCap
            ,ps.medianTEV
            ,ps.medianTEVRevenue
            ,ps.medianTEVEBIT
            ,ps.medianTEVEBITDA
            ,ps.medianDivYield
            ,ps.medianBeta
            ,ps.avgDivYield
            ,ps.avgBeta
            ,ps.numberOfPositions
            ,ps.equityAssets
            ,ps.minMarketCap
            ,ps.maxMarketCap
            ,ps.newPositions
            ,ps.increasedPositions
            ,ps.decreasedPositions
            ,ps.soldOutPositions
            ,ps.percentOfPortfolio
            ,ps.hasPortfolio            
            from targetskma.ciqOwnCompanyInvestStyle cis
            left outer join targetskma.ciqOwnValGrowStyleType vgst
            on cis.valGrowStyleId = vgst.valGrowStyleId
            left outer join targetskma.ciqOwnSizeStyleType sst
            on cis.sizeStyleId = sst.sizeStyleId
            inner join targetskma.ciqCompany c
            on cis.ownerobjectid = c.companyid
            join targetskma.ciqOwnPortfolioStatistics ps on ps.ownerObjectId=c.companyId
            join targetskma.ciqSubType st on ps.subTypeId=st.subtypeId
            where cis.ownerObjectId in ({', '.join([str(id) for id in fund_ids])})
            and ps.subTypeId>1000000 --industry
            """
    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
    
    
    return read_sql_to_df(sql, connection, cursor)             

def get_pit_portfolio_holdings(pid, date, connection = None):
    """
    List the holdings of a fund at a given date (based on most recently available reporting and current prices)

    Args:
        pid (int): ciqOwnCompanyInvestStyle.ownerObjectId e.g. 6160262 SPDR SP500
        date (str): '2018-03-03' (does not need to be a trading day)
        connection (None, optional): Description
    
    Returns:
        sample output
    df:      ownerobjectid                      companyname  securityid          securityname holdingdate  ... optionsheld percentofportfolio  percentofsharesoutstanding shareschanged percentshareschanged
        0          6160262                       Apple Inc.     2590359          Common Stock  2018-02-28  ...        None               3.95                     1.19887     -17682096                -6.77
        1          6160262            Microsoft Corporation     2630412          Common Stock  2018-02-28  ...        None               3.12                     1.18688      -6641480                -6.78
        2          6160262                 Amazon.com, Inc.     2588567          Common Stock  2018-02-28  ...        None               2.61                     0.97864       -344916                -6.79
        3          6160262                   Facebook, Inc.   126910336  Class A Common Stock  2018-02-28  ...        None               1.84                     1.17897      -2053282                -6.78
        4          6160262             JPMorgan Chase & Co.     2622875         Common Shares  2018-02-28  ...        None               1.73                     1.20056      -2987348                -6.77
 
    """

    if isinstance(date, str):
        date = pd.to_datetime(date)

    datestr = date.strftime('%Y-%m-%d')

    sql = f"""
                SELECT ph.ownerObjectId
                ,c.companyName
                ,c.companyid
                ,s.securityId
                ,s.securityName
                ,ph.holdingDate
                ,ph.fromDate
                ,ph.toDate
                ,ph.sharesHeld
                ,ph.optionsHeld
                ,ph.percentOfPortfolio
                ,ph.percentofSharesOutstanding
                ,ph.sharesChanged
                ,ph.percentSharesChanged
                FROM targetskma.ciqOwnPortfolioHolding ph
                INNER JOIN targetskma.ciqSecurity s
                ON ph.securityId = s.securityID
                INNER JOIN targetskma.ciqCompany c
                ON s.companyId = c.companyId
                WHERE ph.ownerObjectId = '{pid}'
                AND ph.fromDate < '{date}'
                AND ph.toDate > '{date}'
                ORDER BY ph.percentOfPortfolio DESC
        """
    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
    
    
    return read_sql_to_df(sql, connection, cursor)             



def get_current_portfolio_holdings(pid, connection = None):
    """
    List the current holdings of a fund (based on most recently available reporting and current prices)
    
    Args:
        pid (int): ciqOwnCompanyInvestStyle.ownerObjectId e.g. 6160262 SPDR SP500
        connection (None, optional): Description
    
    Returns:
        sample output
    df:      ownerobjectid            companyname  companyid  securityid          securityname holdingdate   fromdate weightingdate  sharesheld optionsheld percentofportfolio percentofsharesoutstanding
        0          6160262             Apple Inc.      24937     2590359          Common Stock  2021-02-26 2021-02-26    2021-03-28   162927140        None               5.77                    0.97049
        1          6160262  Microsoft Corporation      21835     2630412          Common Stock  2021-02-26 2021-02-26    2021-03-28    77076370        None               5.33                    1.02193
        2          6160262       Amazon.com, Inc.      18749     2588567          Common Stock  2021-02-26 2021-02-26    2021-03-28     4347844        None               3.88                    0.86341
        3          6160262         Facebook, Inc.   20765463   126910336  Class A Common Stock  2021-02-26 2021-02-26    2021-03-28    24507496        None               2.03                    1.01883
        4          6160262          Alphabet Inc.      29096    11311638  Class A Common Stock  2021-02-26 2021-02-26    2021-03-28     3065043        None               1.81                    1.01918
     
    """


    sql = f"""
            SELECT pl.ownerObjectId
            ,c.companyName
            ,c.companyid
            ,s.securityId
            ,s.securityName
            ,pl.holdingDate
            ,pl.fromDate
            ,pl.weightingDate
            ,pl.sharesHeld
            ,pl.optionsHeld
            ,pl.percentOfPortfolio
            ,pl.percentofSharesOutstanding
            FROM targetskma.ciqOwnPortfolioLatest pl
            INNER JOIN targetskma.ciqSecurity s
            ON pl.securityId = s.securityID
            INNER JOIN targetskma.ciqCompany c
            ON s.companyId = c.companyId
            WHERE pl.ownerObjectId = '{pid}'
            ORDER BY pl.percentOfPortfolio DESC
        """
    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
    
    
    return read_sql_to_df(sql, connection, cursor) 


def get_co_analysts_network(date, lookback = 360, connection = None):
    """Extract analysis coverage (FROM EARNING CALL) given a date range (from date-looback to date)
    currently, DEPRECIATED

    Args:
        date (str): '2018-03-03'
        lookback (int, optional): extract the transcript created within the last int days, e.g. 30 
        connection (None, optional): Description
    
    Returns:
        sample output
    df:        keydevid  companyid  eventtype                  nameofperson                                      titleofperson   personid                                    companyofperson  companyidofperson
        0     234044018      93136         52            Niamh M. Alexander        Managing Director and Senior Vice President   53466734  Keefe, Bruyette, & Woods, Inc., Research Division           25231371
        1     236318717     740535         48          Brian Bertram Bedell  Former Brokers, Trust Banks and Exchanges Analyst   71423533                  ISI Group Inc., Research Division           40309966
        2     242267999     740535         48  Patrick Joseph O'Shaughnessy                                   Research Analyst   53461529  Raymond James & Associates, Inc., Research Div...           40374081
        3     247724640     740535         48        Christopher Meo Harris        Director and Senior Equity Research Analyst   61457441     Wells Fargo Securities, LLC, Research Division            9589274
        4     256956511     740535        192          William Raymond Katz  MD & Global Head of Diversified Financials Sector  100411591                  Citigroup Inc., Research Division           25232403
        ...         ...        ...        ...                           ...                                                ...        ...                                                ...                ...
        2333  554491670    1222734         48                 Brian J Lalli                          Director & Senior Analyst  216340011               Barclays Bank PLC, Research Division            8280347
        2334  554545627     171649        194          James Lloyd Hardiman               Managing Director of Equity Research  279723462         Wedbush Securities Inc., Research Division           26256844
        2335  554562846      30300        194                 Huidong  Wang                                   Research Analyst  536775258               Barclays Bank PLC, Research Division            8280347
        2336  554591912     310545         51          William Raymond Katz  MD & Global Head of Diversified Financials Sector  100411591                  Citigroup Inc., Research Division           25232403
        2337  554617707  273640140         52            Keith Brian Hughes                                  Managing Director   36269886         Truist Securities, Inc., Research Division           27145242
    
    """
    if isinstance(date, str):
        date = pd.to_datetime(date)

    datestr = date.strftime('%Y-%m-%d')
    ## according to SP, the maximum processing time is less than one month, so put OneMonthDelayDatestr for filingdate filter when instance is backfilled
    #OneMonthDelayDatestr = (date - timedelta(days=30)).strftime('%Y-%m-%d')
    # put a timely check to filter out the data that's too old
    TimelyDatestr = (date - timedelta(days=lookback)).strftime('%Y-%m-%d')

    sql = f"""
            SELECT DISTINCT on (t.keyDevId) t.keyDevId
            ,c.companyid
            ,ete.keyDevEventTypeId as EventType
            ,COALESCE(CONCAT(ana.firstName, ' ', ana.middleName, ' ' ,ana.lastName), tp.transcriptPersonName) AS NameOfPerson
            ,ana.title AS TitleOfPerson
            ,tp.proId AS PersonId           
            ,COALESCE(ana.companyName, tp.companyName) AS CompanyOfPerson
            ,ana.companyid AS CompanyIdOfPerson
            FROM targetskma.ciqTranscript t
            JOIN targetskma.ciqTranscriptComponent tc ON tc.transcriptId = t.transcriptId
            JOIN targetskma.ciqTranscriptPerson tp ON tp.transcriptpersonid = tc.transcriptpersonid
            JOIN (
                select pr.proId, p.firstName, p.middleName, p.lastName, pr.title, comp.companyName, comp.companyid
                from targetskma.ciqProfessional pr
                JOIN targetskma.ciqPerson p on pr.personId = p.personId
                JOIN targetskma.ciqCompany comp on pr.companyid = comp.companyId
            ) ana on tp.proId= ana.proId
            JOIN targetskma.ciqEvent e ON e.keyDevId = t.keyDevId
            JOIN targetskma.ciqEventToObjectToEventType ete ON ete.keyDevId = t.keyDevId
            JOIN targetskma.ciqCompany c ON c.companyId = ete.objectId
            WHERE t.transcriptCreationDateUTC >= '{TimelyDatestr}'
            AND t.transcriptCreationDateUTC <= '{datestr}'  
            AND tc.transcriptComponentTypeId = 3 -- Question
            AND tp.speakerTypeId = 3 -- Analyst
            AND c.companyid in (
                SELECT c.companyID
                from targetskma.ciqsecurityidentifier idm
                join targetskma.ciqSecurity s on s.securityId=idm.securityId
                join targetskma.ciqCompany c on c.companyID=s.companyID
                join targetskma.ciqTradingItem ti on ti.securityId=s.securityId
                join targetskma.ciqExchange ec on ti.exchangeid = ec.exchangeid
                WHERE c.countryid = 213 
                and s.securitySubTypeId=1
                and s.primaryflag=1
                and ti.primaryflag=1
                and ti.currencyid=160
                and ec.countryid=213
            )       
    """
    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
        
    return read_sql_to_df(sql, connection, cursor) 


def get_transcript_ref_earliest(ls_ids, startdate, enddate=None, connection = None):
    """
    Get historical reference table for a list of companyid with asscoiated transcriptid from given a date range (from startdate to enddate)

    Args:
        ls_ids (list): list of company id, e.g. [6631173, 429233, 33414482, 262353065, 1600081]
        startdate (str): '2021-03-03'
        enddate (str, optional): '2021-03-30'
        connection (None, optional): Description
    
    Returns:
       sample output
    df:      transcriptid transcriptcreationdateutc  companyid                   companyname   keydevid  ...  fiscalquarter delayreasontypeid delayreasontypename iscancelledflag delayreasonnotes
        0         2228812       2021-03-03 00:15:52    6631173               B&G Foods, Inc.  704677961  ...              4               NaN                None             NaN             None
        1         2228824       2021-03-03 00:37:01     429233       TransMedics Group, Inc.  704570793  ...              4               NaN                None             NaN             None
        2         2228846       2021-03-03 01:03:12   33414482            Veeva Systems Inc.  704483765  ...              4               NaN                None             NaN             None
        3         2228848       2021-03-03 01:05:38  262353065                 Vectrus, Inc.  704552829  ...              4               NaN                None             NaN             None
        4         2228851       2021-03-03 01:10:43    1600081  Grocery Outlet Holding Corp.  704570277  ...              4               NaN                None             NaN             None

    """
    
    startdatestr = (
        pd.to_datetime(startdate)
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
    )
    
    if enddate is None:
        enddate = datetime.now()
        
    enddatestr = (
        pd.to_datetime(enddate)
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
    )
    
    
    sql = f"""
            SELECT t.transcriptId, t.transcriptCreationDateUTC, comp.companyId, comp.companyName, t.keyDevId, t.transcriptCollectionTypeId,
            e.mostImportantDateUTC as EarningsDateUTC, e.announcedDateUTC, ct.transcriptCollectionTypeName, e.headline,
            eb.fiscalyear, eb.fiscalquarter, dr.delayReasonTypeId, drt.delayReasonTypeName, drt.isCancelledFlag, dr.delayReasonNotes
            FROM targetskma.ciqTranscript t
            JOIN targetskma.ciqEvent e ON e.keyDevId = t.keyDevId
            JOIN targetskma.ciqEventToObjectToEventType ete ON ete.keyDevId = t.keyDevId
            JOIN targetskma.ciqCompany comp ON comp.companyId = ete.objectId
            JOIN targetskma.ciqTranscriptCollectionType ct ON ct.transcriptCollectionTypeId = t.transcriptCollectionTypeId
            JOIN targetskma.ciqEventType et ON et.keyDevEventTypeId = ete.keyDevEventTypeId
            JOIN targetskma.ciqeventcallbasicinfo eb on eb.keyDevId = t.keyDevId
        	LEFT JOIN targetskma.ciqTranscriptDelayReason dr on dr.keyDevId = t.keyDevId
        	LEFT JOIN targetskma.ciqTranscriptDelayReasonType drt on dr.delayReasonTypeId = drt.delayReasonTypeId            
            WHERE et.keyDevEventTypeId='48' --Earnings Calls
            AND t.transcriptId in (SELECT DISTINCT a.transcriptId FROM targetskma.ciqTranscript a,
            (
            SELECT keyDevId,Min(transcriptCreationDateUTC) as minTm FROM targetskma.ciqTranscript
            WHERE transcriptCollectionTypeId!=7
            AND transcriptCreationDateUTC >= '{startdatestr}'
            AND transcriptCreationDateUTC <= '{enddatestr}'   
            GROUP BY keyDevId
            ) mtm
            WHERE a.keyDevId = mtm.keyDevId AND a.transcriptCreationDateUTC = mtm.minTm)
            AND comp.companyid in ({', '.join([str(id) for id in ls_ids])}) 
            AND t.transcriptCreationDateUTC >= '{startdatestr}'
            AND t.transcriptCreationDateUTC <= '{enddatestr}'           
            ORDER BY t.transcriptCreationDateUTC asc;
            """    
    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
                
    return read_sql_to_df(sql, connection, cursor) 


def get_transcript(ls_transcript_ids, connection = None):
    """Get transcript given a list of transcript ids
    
    Args:
        ls_transcript_ids (list): list of transcriptid e.g. [2228812, ]
        connection (None, optional): Description
    
    Returns:
        sample output
    df:     transcriptcomponentid  transcriptid  componentorder  transcriptcomponenttypeid  ...           transcriptcomponenttypename transcriptpersonname speakertypename                              title
        0                86555566       2228812               0                          1  ...         Presentation Operator Message             Operator        Operator                               None
        1                86555567       2228812               1                          2  ...                      Presenter Speech         David Wenner      Executives  Interim President, CEO & Director
        2                86555568       2228812               2                          2  ...                      Presenter Speech          Bruce Wacha      Executives      CFO & Executive VP of Finance
        3                86555569       2228812               3                          2  ...                      Presenter Speech         David Wenner      Executives  Interim President, CEO & Director
        4                86555570       2228812               4                          7  ...  Question and Answer Operator Message             Operator        Operator                               None

    """
    sql = f"""
    select tc.transcriptComponentId, tc.transcriptId, tc.componentOrder, tc.transcriptComponentTypeId, 
    tc.transcriptPersonId, CAST(tc.componentText AS TEXT) AS componentText, tct.transcriptComponentTypeName, tp.transcriptPersonName, tst.speakerTypeName, pb.title
    from targetskma.ciqTranscriptComponent tc
    LEFT JOIN targetskma.ciqTranscriptPerson tp on tc.transcriptPersonId = tp.transcriptPersonId
    LEFT JOIN targetskma.ciqTranscriptSpeakerType tst on tst.speakerTypeId = tp.speakerTypeId
    LEFT JOIN targetskma.ciqProfessional pb on pb.proId= tp.proId
    LEFT JOIN targetskma.ciqTranscriptComponentType tct on tc.transcriptComponentTypeId = tct.transcriptComponentTypeId
    where transcriptId in ({', '.join([str(id) for id in ls_transcript_ids])}) 
    """
    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
        
    return read_sql_to_df(sql, connection, cursor) 


def get_latest_pricing(asofdate, sec_ids, is_eod = False, connection = None):
    """
    TODO: what about split during real time
    get the transcripts of given companies from start to end
    :param asofdate: start date (incl.)
    :param sec_ids: list of secid
    :return:
    """

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
    if is_eod:
        # the close is available
        enddatestr = (
            pd.to_datetime(asofdate)
                .tz_localize(SERVER_TIMEZONE)
                .tz_convert("UTC")
                .strftime("%Y-%m-%d")
        )
    else:
        # mainly for backtesting and update history (mimic behavior at the morning)
        enddatestr = (
            (pd.to_datetime(asofdate) - timedelta(days = 1))
                .tz_localize(SERVER_TIMEZONE)
                .tz_convert("UTC")
                .strftime("%Y-%m-%d")
        )

    startdatestr = ((pd.to_datetime(asofdate) - timedelta(days = 5))
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
            )
 

    sql = """
    CREATE TEMP TABLE temp_universe
    (
        securityid int
    )
    ON COMMIT DELETE ROWS;
    BEGIN TRANSACTION;
    """
    cursor.execute(sql)

    sql = """SELECT * FROM information_schema.tables"""
    cursor.execute(sql)
    data = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    schema = pd.DataFrame(data,columns = columns).table_schema.unique()
    for i in schema:
        if 'pg_temp' in i:
            tschema = i

    sec_list = ""
    for sid in sec_ids:
        sec_list += "INSERT INTO %s.temp_universe VALUES (%s);\n"%(tschema,sid)

    sql = f"""
    {sec_list}
    SELECT s.securityId
    ,pe.pricingDate
    ,pe.priceClose
    ,pe.priceOpen
    ,pe.priceHigh
    ,pe.priceLow
    ,pe.volume
    ,(pe.priceClose*COALESCE(daf.divAdjFactor,1)) divAdjPrice
    ,COALESCE(daf.divAdjFactor,1) as divAdjFactor
    from targetskma.ciqSecurity s
    join targetskma.ciqTradingItem ti on ti.securityId=s.securityId
    join targetskma.ciqPriceEquity pe on pe.tradingItemId=ti.tradingItemId
    left join targetskma.ciqPriceEquityDivAdjFactor daf on pe.tradingItemId=daf.tradingItemId
    and daf.fromDate<=pe.pricingDate --Find dividend adjustment factor on pricing date
    and (daf.toDate is null or daf.toDate>=pe.pricingDate)
    WHERE s.securityId in (SELECT securityid FROM {tschema}.temp_universe)
    and s.primaryflag=1
    and ti.primaryflag=1
    and pe.pricingDate>='{startdatestr}'
    and pe.pricingDate<='{enddatestr}'
    ORDER BY pe.pricingDate asc
    """
 
    pr = read_sql_to_df(sql, connection, cursor)   
    pr = pr.drop_duplicates(['securityid'], keep = 'last')
    pr.loc[:,'divadjfactor'] = pr.loc[:,'divadjfactor'].astype(float).fillna(1)
    pr.loc[:,'divadjprice'] = pr.loc[:,'divadjprice'].astype(float)
    pr.loc[:,'priceclose'] = pr.loc[:,'priceclose'].astype(float) * pr.loc[:,'divadjfactor']
    pr.loc[:,'priceopen'] = pr.loc[:,'priceopen'].astype(float) * pr.loc[:,'divadjfactor']
    pr.loc[:,'pricehigh'] = pr.loc[:,'pricehigh'].astype(float) * pr.loc[:,'divadjfactor']
    pr.loc[:,'pricelow'] = pr.loc[:,'pricelow'].astype(float) * pr.loc[:,'divadjfactor']
    pr.loc[:,'volume'] = pr.loc[:,'volume'].astype(float)

    return pr#pd.pivot_table(pr, values='divadjprice', index = 'pricingdate', columns = 'companyid', aggfunc='first')    


def get_historical_marketcap(cids, start_date = '2010-01-01', end_date = '2021-01-01', connection = None):
    """
    author: zheng Apr.29.2021
    Args:
        cids(list): list of companyid 
    
    Returns:
        sample output
    """

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()

    sql = f"""
        select companyid, pricingdate, marketcap
        from ciqmarketcap 
        where 1 = 1 
        and companyId in ({', '.join([str(id) for id in cids])})
        and pricingdate >= '{start_date}'
        and pricingdate <= '{end_date}';

    """
    df = read_sql_to_df(sql, connection, cursor)  
    df.loc[:,'marketcap'] = df.loc[:,'marketcap'].astype(float) 
    return df

def get_latest_marketcap(asofdate, cids, is_eod = False, connection = None):
    """
    TODO: what about split during real time

    Args:
        asofdate (str): as of date when we extract the companyid '2020-03-03'
        cids (list): list of companyids [18671, 18711, 18749]
        is_eod (bool, optional): if is_eod is False (Defualt), we will extract most recent marketcap up until yesterday (escpecially useful for backtest purpose) 
                                           is True, we will extract most recent marketcap up until today 
        connection (None, optional): Description
    
    Returns:
        sample output:
    df:       companyid      marketcap
        0         18671    8953.179035
        1         18711   35706.660720
        2         18749  972696.717053
        3         18833   10023.651813
        4         19033    1753.790643
    """

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
    if is_eod:
        # the close is available
        enddatestr = (
            pd.to_datetime(asofdate)
                .tz_localize(SERVER_TIMEZONE)
                .tz_convert("UTC")
                .strftime("%Y-%m-%d")
        )
    else:
        # mainly for backtesting and update history (mimic behavior at the morning)
        enddatestr = (
            (pd.to_datetime(asofdate) - timedelta(days = 1))
                .tz_localize(SERVER_TIMEZONE)
                .tz_convert("UTC")
                .strftime("%Y-%m-%d")
        )

    startdatestr = ((pd.to_datetime(asofdate) - timedelta(days = 5))
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
            )
 

    sql = """
    CREATE TEMP TABLE temp_universe
    (
        companyid int
    )
    ON COMMIT DELETE ROWS;
    BEGIN TRANSACTION;
    """
    cursor.execute(sql)

    sql = """SELECT * FROM information_schema.tables"""
    cursor.execute(sql)
    data = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    schema = pd.DataFrame(data,columns = columns).table_schema.unique()
    for i in schema:
        if 'pg_temp' in i:
            tschema = i

    sec_list = ""
    for cid in cids:
        sec_list += "INSERT INTO %s.temp_universe VALUES (%s);\n"%(tschema,cid)


    sql = f"""
    {sec_list}
    select mc.companyID, mc.marketcap
    from targetskma.ciqMarketCap mc
    join (
        select companyID, max(pricingdate) as MaxDate
        from targetskma.ciqMarketCap
        where pricingdate <= '{enddatestr}'
        and pricingDate>='{startdatestr}'
        group by companyID
    ) tm on mc.companyID = tm.companyID and mc.pricingdate = tm.MaxDate
    WHERE mc.companyID in (SELECT companyid FROM {tschema}.temp_universe)
    """
    df = read_sql_to_df(sql, connection, cursor)  
    df.loc[:,'marketcap'] = df.loc[:,'marketcap'].astype(float) 
    return df

def get_industry(cids, connection = None):
    """
    return the simpleindustrycode for given company id list
    
    Args:
        cids (list): list of companyids [18671, 18711, 18749]
        connection (None, optional): Description
    
    Returns:
        sample output
    df:       companyid  simpleindustryid
        0     225890957                13
        1      30487348                 1
        2       4245259                60
        3      23246157                50
        4     126751161                56
    
    """

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()

 

    sql = """
    CREATE TEMP TABLE temp_universe
    (
        companyid int
    )
    ON COMMIT DELETE ROWS;
    BEGIN TRANSACTION;
    """
    cursor.execute(sql)

    sql = """SELECT * FROM information_schema.tables"""
    cursor.execute(sql)
    data = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    schema = pd.DataFrame(data,columns = columns).table_schema.unique()
    for i in schema:
        if 'pg_temp' in i:
            tschema = i

    sec_list = ""
    for cid in cids:
        sec_list += "INSERT INTO %s.temp_universe VALUES (%s);\n"%(tschema,cid)


    sql = f"""
    {sec_list}
    select mc.companyID, mc.simpleindustryid
    from targetskma.ciqCompany mc
    WHERE mc.companyID in (SELECT companyid FROM {tschema}.temp_universe)
    """
    return read_sql_to_df(sql, connection, cursor)   

def get_sec_pricing(start, end, sec_ids, connection = None):
    """
    get the transcripts of given companies from start to end
    :param start: start date (incl.)
    :param end: end date (incl.)
    :param sec_ids: list of secid
    :return:
    """

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()

    startdatestr = (
        pd.to_datetime(start)
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
    )
    enddatestr = (
        pd.to_datetime(end)
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
    )

    sql = """
    CREATE TEMP TABLE temp_universe
    (
        securityid int
    )
    ON COMMIT DELETE ROWS;
    BEGIN TRANSACTION;
    """
    cursor.execute(sql)

    sql = """SELECT * FROM information_schema.tables"""
    cursor.execute(sql)
    data = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    schema = pd.DataFrame(data,columns = columns).table_schema.unique()
    for i in schema:
        if 'pg_temp' in i:
            tschema = i

    company_list = ""
    for sid in sec_ids:
        company_list += "INSERT INTO %s.temp_universe VALUES (%s);\n"%(tschema,sid)

    sql = f"""
    {company_list}
    SELECT s.securityid
    ,pe.pricingDate
    ,pe.priceClose
    ,pe.priceOpen
    ,pe.priceHigh
    ,pe.priceLow
    ,pe.volume
    ,(pe.priceClose*COALESCE(daf.divAdjFactor,1)) divAdjPrice
    ,COALESCE(daf.divAdjFactor,1) as divAdjFactor
    from targetskma.ciqSecurity s 
    join targetskma.ciqTradingItem ti on ti.securityId=s.securityId
    join targetskma.ciqPriceEquity pe on pe.tradingItemId=ti.tradingItemId
    left join targetskma.ciqPriceEquityDivAdjFactor daf on pe.tradingItemId=daf.tradingItemId
    and daf.fromDate<=pe.pricingDate --Find dividend adjustment factor on pricing date
    and (daf.toDate is null or daf.toDate>=pe.pricingDate)
    WHERE s.securityid in (SELECT securityid FROM {tschema}.temp_universe)
    and s.primaryflag=1
    and ti.primaryflag=1
    and pe.pricingDate>='{startdatestr}'
    and pe.pricingDate<='{enddatestr}'
    ORDER BY pe.pricingDate asc
    """
 
    pr = read_sql_to_df(sql, connection, cursor)   
    
    return pr#pd.pivot_table(pr, values='divadjprice', index = 'pricingdate', columns = 'companyid', aggfunc='first')    




def get_transcript_ref_by_transcriptid(transcriptid, connection = None):
    """Get historical reference table for a list of transcriptids from the startdate (NOT THE TRANSCRIPT CONTENT)

    Args:
        transcriptid (list): list of transcriptids, e.g. [2228812, ]
        connection (None, optional): Description
    
    Returns:
        sample output
    df:    transcriptid transcriptcreationdateutc  companyid      companyname   keydevid  transcriptcollectiontypeid     earningsdateutc  ...                                           headline fiscalyear fiscalquarter  delayreasontypeid  delayreasontypename iscancelledflag delayreasonnotes
        0       2228812       2021-03-03 00:15:52    6631173  B&G Foods, Inc.  704677961                           2 2021-03-02 21:30:00  ...  B&G Foods, Inc., Q4 2020 Earnings Call, Mar 02...       2020             4               None                 None            None             None

    """
    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
                    
    sql = """
    CREATE TEMP TABLE temp_tids
    (
        transcriptid int
    )
    ON COMMIT DELETE ROWS;
    BEGIN TRANSACTION;
    """
    cursor.execute(sql)

    sql = """SELECT * FROM information_schema.tables"""
    cursor.execute(sql)
    data = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    schema = pd.DataFrame(data,columns = columns).table_schema.unique()
    for i in schema:
        if 'pg_temp' in i:
            tschema = i

    tids = ""
    for tid in transcriptid:
        tids += "INSERT INTO %s.temp_tids VALUES (%s);\n"%(tschema,tid)

    
    sql = f"""
            {tids}
            SELECT t.transcriptId, t.transcriptCreationDateUTC, comp.companyId, comp.companyName, t.keyDevId, t.transcriptCollectionTypeId,
            e.mostImportantDateUTC as EarningsDateUTC, e.announcedDateUTC, ct.transcriptCollectionTypeName, e.headline,
            eb.fiscalyear, eb.fiscalquarter, dr.delayReasonTypeId, drt.delayReasonTypeName, drt.isCancelledFlag, dr.delayReasonNotes
            FROM targetskma.ciqTranscript t
            JOIN targetskma.ciqEvent e ON e.keyDevId = t.keyDevId
            JOIN targetskma.ciqEventToObjectToEventType ete ON ete.keyDevId = t.keyDevId
            JOIN targetskma.ciqCompany comp ON comp.companyId = ete.objectId
            JOIN targetskma.ciqTranscriptCollectionType ct ON ct.transcriptCollectionTypeId = t.transcriptCollectionTypeId
            JOIN targetskma.ciqEventType et ON et.keyDevEventTypeId = ete.keyDevEventTypeId
            JOIN targetskma.ciqeventcallbasicinfo eb on eb.keyDevId = t.keyDevId
            LEFT JOIN targetskma.ciqTranscriptDelayReason dr on dr.keyDevId = t.keyDevId
            LEFT JOIN targetskma.ciqTranscriptDelayReasonType drt on dr.delayReasonTypeId = drt.delayReasonTypeId            
            WHERE et.keyDevEventTypeId='48' --Earnings Calls
            AND t.transcriptId in (select transcriptid FROM {tschema}.temp_tids)        
            ORDER BY t.transcriptCreationDateUTC asc;
            """    

    return read_sql_to_df(sql, connection, cursor) 


def get_current_index_values(connection = None):
    """Get all index historical pricing
    
    Args:
        connection (None, optional): Description

    
    Returns:
    df:       indexname  indexid  tradingitemid  valuedate                value
        0       S&P 500  2668699        2633671 1928-01-03    17.76000000000000
        1       S&P 500  2668699        2633671 1928-01-04    17.72000000000000
        2       S&P 500  2668699        2633671 1928-01-05    17.55000000000000
        3       S&P 500  2668699        2633671 1928-01-06    17.66000000000000
        4       S&P 500  2668699        2633671 1928-01-07    17.68000000000000

    """
    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()

    sql = """
          select i.indexName
          ,i.indexid

          ,it.tradingItemID
          ,v.valueDate
          ,v.value
          from ciqIndex i
          inner join ciqIndexTradingItem it
          on i.indexID = it.indexID
          inner join ciqIndexValue v
          on v.tradingItemID = it.tradingItemID
          and v.dataItemId=112099
          where it.tradingItemID = 2633671 --S&P 500
          order by v.valueDate asc
        """      
    return read_sql_to_df(sql, connection, cursor) 

def get_current_index_constituents(index_id, connection = None):
    """Get current index constituents given index id (WE DONT HAVE HISTORICAL ETF CONSTITUENTS)
    
    Args:
        index_id (int): indexid of our interest, e.g. 'S&P 500' --> 2668699
        connection (None, optional): Description
    
    Returns:
        sample output:
    df:     indexname  indexid  constituentid   fromdate todate  tradingitemid  pacvertofeedpop
        0     S&P 500  2668699      102087555 2014-08-07   None       49031561              982
        1     S&P 500  2668699      102185496 2009-12-18   None        2586086              982
        2     S&P 500  2668699      102185499 2009-12-18   None        2586130              982
        3     S&P 500  2668699      102185518 2009-12-18   None        2586533              982
        4     S&P 500  2668699      102185520 2018-05-31   None        2586597              982
    """
    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()

    sql = f"""
            select indexName, t.*
            from targetskma.ciqIndex a, targetskma.ciqIndexConstituent t
            where a.indexID=t.indexID
            and a.indexID = {index_id}
            and toDate is null
          """

    return read_sql_to_df(sql, connection, cursor) 


def get_est_analysts_network(date, lookback = 180, connection = None):
    """
    Get all analyst network (references) in the time range (date-lookback, date), 

    Args:
        date (str): as of date, '2020-03-03'
        lookback (int, optional): lookback horizon, by days, default 180
        connection (None, optional): Description
    
    Returns:
        sample output:
    df:        companyid  estimateanalystid  estimatebrokerid
        0         135613              16468                38
        1         135613              36073               103
        2         135613              30423               112
        3         135613              11639               115
        4         135613               2948               147
    """

    if isinstance(date, str):
        date = pd.to_datetime(date)

    datestr = date.strftime('%Y-%m-%d')
    ## according to SP, the maximum processing time is less than one month, so put OneMonthDelayDatestr for filingdate filter when instance is backfilled
    #OneMonthDelayDatestr = (date - timedelta(days=30)).strftime('%Y-%m-%d')
    # put a timely check to filter out the data that's too old
    TimelyDatestr = (date - timedelta(days=lookback)).strftime('%Y-%m-%d')



    sql = f"""
            select companyid,estimateanalystid,estimatebrokerid from targetskma.ciqEstimateCoverage eco
            WHERE eco.tradingItemId is null
            AND eco.analystoriginaldate <= '{TimelyDatestr}'
            AND eco.analystexpirationdate >= '{datestr}'  
            AND eco.companyid in (
                SELECT c.companyID
                from targetskma.ciqsecurityidentifier idm
                join targetskma.ciqSecurity s on s.securityId=idm.securityId
                join targetskma.ciqCompany c on c.companyID=s.companyID
                join targetskma.ciqTradingItem ti on ti.securityId=s.securityId
                join targetskma.ciqExchange ec on ti.exchangeid = ec.exchangeid
                WHERE c.countryid = 213 
                and s.securitySubTypeId=1
                and s.primaryflag=1
                and ti.primaryflag=1
                and ti.currencyid=160
                and ec.countryid=213
            )   
        """
    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
        
    return read_sql_to_df(sql, connection, cursor) 




def get_traded_information_given_cid(companyid, connection = None):
    """
    get the basic information from company id to corresponding isin
    
    Args:
        companyid (int): company id e.g. 135613
        connection (None, optional): Description
    
    Returns:
        sample output:
    df:            isin  securityid  securityname  tradingitemid  companyid     companyname
        0  US59509C1053    27127713  Common Stock       27127729     135613  Micromet, Inc.

    """
    sql = f"""
	SELECT 
	idm.identifiervalue as isin
	, s.securityid
	, s.securityname
	, ti.tradingitemID
	, cpny.companyID
	, cpny.companyname
	FROM
	ciqsecurityidentifier AS idm
	INNER JOIN ciqSecurity AS s ON s.securityId=idm.securityId
	INNER JOIN ciqCompany AS cpny on cpny.companyID=s.companyID
	INNER JOIN ciqTradingItem AS ti ON ti.securityId=s.securityId
	INNER JOIN ciqExchange AS ec ON ti.exchangeid = ec.exchangeid
	WHERE cpny.companyID = {companyid}
	AND s.primaryflag=1
	AND
	ti.primaryflag=1
    """
    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
    return read_sql_to_df(sql, connection, cursor).drop_duplicates('securityid')

############################################## new ########################################################### 
# apr.1. 2021 
# @author: zheng
def get_cur_fundamental(ls_ids, ls_dataitemid, connection = None):


    """
    @author: zheng
    reWrite from function get_PIT_fundamental
    reference: CIQ Premium Financials Sample Appendix B, P140

    get fundamentals finacial from CIQ PIT premium financials
    CIQ PIT database has a backfill mechanism causing some instance dates to be much later than filing date for earlier years
    but normally the maximum processing time is less than a month, so assume any lag greater than a month is a backfill data


    select a.dataItemName, a.dataItemId from ciqDataItem a inner join ciqFinCollectionData b
    on a.dataItemId=b.dataItemId where a.dataItemName like '%split%'
    Args:
            companyid (list): list of company id e.g. [6631173, 429233, 33414482, 262353065, 1600081]
            ls_dataitemid (list): dataitemid: dataitemid in table ciqDataItem, e.g.
                                            1100 is common shares outstanding (adjusted)
                                            4379 is normalized basic EPS (comparable to consensus)
                                            112987 is book value
            connection (None, optional): Description

    Returns:
        sample output:
                    companyname  companyid periodenddate filingdate formtype periodtypename  calendarquarter  calendaryear  dataitemid               dataitemname dataitemvalue        instancedate
0                 Vectrus, Inc.  262353065    2020-12-31 2021-03-02     10-K      Quarterly                4          2020        1100  Common Shares Outstanding     11.624717 2021-03-03 08:09:00
1               B&G Foods, Inc.    6631173    2021-01-02 2021-03-02     10-K      Quarterly                4          2020        1100  Common Shares Outstanding     64.252859 2021-03-03 22:30:00
2  Grocery Outlet Holding Corp.    1600081    2021-01-02 2021-03-02     10-K      Quarterly                4          2020        1100  Common Shares Outstanding     94.854336 2021-03-04 09:13:00
3       TransMedics Group, Inc.     429233    2020-12-31 2021-03-11     10-K      Quarterly                4          2020        1100  Common Shares Outstanding     27.175305 2021-03-13 03:26:00

    """    

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()

    sql = f"""
            SELECT 
            c.companyName, 
            c.companyId, 
            fi.periodEndDate,
            fi.filingDate,
            fi.formtype,
            pt.periodTypeName,
            fp.calendarQuarter, 
            fp.calendarYear,
            fd.dataItemId,
            di.dataItemName,
            fd.dataItemValue,
            fid.instanceDate 
            
            FROM ciqCompany c 
            join ciqFinPeriod fp on fp.companyId = c.companyId 
            join ciqPeriodType pt on pt.periodTypeId = fp.periodTypeId 
            join ciqFinInstance fi on fi.financialPeriodId = fp.financialPeriodId 
            join ciqFinInstanceDate fid on fid.financialInstanceId = fi.financialInstanceId
            join ciqFinInstanceToCollection ic on ic.financialInstanceId = fi.financialInstanceId 
            join ciqFinCollection fc on fc.financialCollectionId = ic.financialCollectionId 
            join ciqFinCollectionData fd on fd.financialCollectionId = fc.financialCollectionId 
            join ciqDataItem di on di.dataItemId = fd.dataItemId 
            
            WHERE fd.dataItemId in ({', '.join([str(id) for id in ls_dataitemid])})  
            AND    c.companyId in ({', '.join([str(id) for id in ls_ids])})
            AND    fp.periodTypeId = 2 --quarterly 
            AND    fi.latestForFinancialPeriodFlag = 1 --Latest Instance For Financial Period 
            AND    fp.latestPeriodFlag = 1  --Current Period 
            
            -- ORDER BY   di.dataItemName
        """
    sql = f"""
            SELECT 
            fi.*
            
            FROM ciqCompany c 
            join ciqFinPeriod fp on fp.companyId = c.companyId 
            join ciqPeriodType pt on pt.periodTypeId = fp.periodTypeId 
            join ciqFinInstance fi on fi.financialPeriodId = fp.financialPeriodId 
            join ciqFinInstanceDate fid on fid.financialInstanceId = fi.financialInstanceId
            join ciqFinInstanceToCollection ic on ic.financialInstanceId = fi.financialInstanceId 
            join ciqFinCollection fc on fc.financialCollectionId = ic.financialCollectionId 
            join ciqFinCollectionData fd on fd.financialCollectionId = fc.financialCollectionId 
            join ciqDataItem di on di.dataItemId = fd.dataItemId 
            
            WHERE fd.dataItemId in ({', '.join([str(id) for id in ls_dataitemid])})  
            AND    c.companyId in ({', '.join([str(id) for id in ls_ids])})
            AND    fp.periodTypeId = 2 --quarterly 
            AND    fi.latestForFinancialPeriodFlag = 1 --Latest Instance For Financial Period 
            AND    fp.latestPeriodFlag = 1  --Current Period 
            
            -- ORDER BY   di.dataItemName
        """
    return read_sql_to_df(sql, connection, cursor)       

def get_backwards_fundamental(ls_ids, ls_dataitemid, calendaryear, connection = None):


    """
    @author: zheng
    Returns:
        sample output:
                    companyname  companyid periodenddate filingdate formtype periodtypename  calendarquarter  calendaryear  dataitemid               dataitemname dataitemvalue        instancedate
0                 Vectrus, Inc.  262353065    2020-12-31 2021-03-02     10-K      Quarterly                4          2020        1100  Common Shares Outstanding     11.624717 2021-03-03 08:09:00
1               B&G Foods, Inc.    6631173    2021-01-02 2021-03-02     10-K      Quarterly                4          2020        1100  Common Shares Outstanding     64.252859 2021-03-03 22:30:00
2  Grocery Outlet Holding Corp.    1600081    2021-01-02 2021-03-02     10-K      Quarterly                4          2020        1100  Common Shares Outstanding     94.854336 2021-03-04 09:13:00
3       TransMedics Group, Inc.     429233    2020-12-31 2021-03-11     10-K      Quarterly                4          2020        1100  Common Shares Outstanding     27.175305 2021-03-13 03:26:00

    """    

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()

    sql = f"""
            SELECT 
            -- c.companyName, 
            c.companyId, 
            fi.periodEndDate,
            -- fi.filingDate,
            -- fi.formtype,
            -- pt.periodTypeName,
            fp.calendarQuarter, 
            fp.calendarYear,
            fd.dataItemId,
            -- di.dataItemName,
            fd.dataItemValue
            -- fid.instanceDate 
            
            FROM ciqCompany c 
            join ciqFinPeriod fp on fp.companyId = c.companyId 
            join ciqPeriodType pt on pt.periodTypeId = fp.periodTypeId 
            join ciqFinInstance fi on fi.financialPeriodId = fp.financialPeriodId 
            join ciqFinInstanceDate fid on fid.financialInstanceId = fi.financialInstanceId
            join ciqFinInstanceToCollection ic on ic.financialInstanceId = fi.financialInstanceId 
            join ciqFinCollection fc on fc.financialCollectionId = ic.financialCollectionId 
            join ciqFinCollectionData fd on fd.financialCollectionId = fc.financialCollectionId 
            join ciqDataItem di on di.dataItemId = fd.dataItemId 
            
            WHERE fd.dataItemId in ({', '.join([str(id) for id in ls_dataitemid])})  
            AND    c.companyId in ({', '.join([str(id) for id in ls_ids])})
            AND    fp.periodTypeId = 2 --quarterly 

            AND    fi.latestForFinancialPeriodFlag = 1 --BEST Instance For Each Financial Period 
            AND    fp.calendarYear in ({', '.join([str(id) for id in calendaryear])})

            -- ORDER BY   di.dataItemName
        """
    
    return read_sql_to_df(sql, connection, cursor)       


def get_historical_fundamental(ls_ids, ls_dataitemid, periodtypeid = [1, 2], startyear = 2007, startdate = '2007-01-01', connection = None):


    """
    @author: zheng
    """    

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()

    sql = f"""
            SELECT 
            fp.companyId, 
            fi.periodEndDate,
            fi.filingDate,
            fi.formtype,
            fi.currencyid,
            fp.periodTypeId, 
            fp.calendarQuarter, 
            fp.fiscalQuarter, 
            fp.calendarYear,
            fp.fiscalYear,
            fd.dataItemId,
            fd.dataItemValue,
            fid.instanceDate 
            
            FROM ciqFinPeriod fp 
            join ciqFinInstance fi on fi.financialPeriodId = fp.financialPeriodId 
            join ciqFinInstanceDate fid on fid.financialInstanceId = fi.financialInstanceId
            join ciqFinInstanceToCollection ic on ic.financialInstanceId = fi.financialInstanceId 
            join ciqFinCollectionData fd on fd.financialCollectionId = ic.financialCollectionId 
            
            WHERE fd.dataItemId in ({', '.join([str(id) for id in ls_dataitemid])})  
            AND    fp.companyId in ({', '.join([str(id) for id in ls_ids])})
            AND    fp.calendarYear >= '{startyear}'
            AND    fp.periodTypeId in ({', '.join([str(id) for id in periodtypeid])}) --quarterly 
            AND  fi.periodEndDate >= '{startdate}'
            
        """
    
    return read_sql_to_df(sql, connection, cursor)           

def search_fundamental(pattern = 'Book', connection = None):

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()

    sql = f"""
            SELECT 
            c.companyName, 
            c.companyId, 
            fi.periodEndDate,
            fi.filingDate,
            fi.formtype,
            pt.periodTypeName,
            fp.calendarQuarter, 
            fp.calendarYear,
            fd.dataItemId,
            di.dataItemName,
            fd.dataItemValue
            
            FROM ciqCompany c 
            join ciqFinPeriod fp on fp.companyId = c.companyId 
            join ciqPeriodType pt on pt.periodTypeId = fp.periodTypeId 
            join ciqFinInstance fi on fi.financialPeriodId = fp.financialPeriodId 
            join ciqFinInstanceToCollection ic on ic.financialInstanceId = fi.financialInstanceId 
            join ciqFinCollection fc on fc.financialCollectionId = ic.financialCollectionId 
            join ciqFinCollectionData fd on fd.financialCollectionId = fc.financialCollectionId 
            join ciqDataItem di on di.dataItemId = fd.dataItemId 
            
            WHERE 1=1
            AND    UPPER(di.dataItemName) like UPPER('%{pattern}%') -- ignore case
            AND    c.companyId = 24937 -- Apple Inc.
            AND    fp.periodTypeId = 2 --quarterly 
            AND    fi.latestForFinancialPeriodFlag = 1 --Latest Instance For Financial Period 
            AND    fp.latestPeriodFlag = 1  --Current Period 
            
        """
    
    return read_sql_to_df(sql, connection, cursor)  
    
def get_cur_miadj_pricing(asofdate, ls_ids, is_eod = True, connection = None):
    """
    Get as of date price data given a series of company ids, using miadjusted table 
    instead of ciqpeequity table 
    
    Args:
        asofdate (str): '2021-05-14'
        ls_ids (list): list of companyid   [11686323, ]
        connection (None, optional): Description
    
    Returns:
        sample ouput: 
   		companyid  tradingitemid   pricedate       priceclose        priceopen        pricehigh         pricelow                volume
	     0      25026        2590705  2021-05-14   61.46000000000   61.19000000000   62.38000000000   60.25000000000    403937.00000000000

    """

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()

    if is_eod:
        # the close is available
        enddatestr = (
            pd.to_datetime(asofdate)
                .tz_localize(SERVER_TIMEZONE)
                .tz_convert("UTC")
                .strftime("%Y-%m-%d")
        )
    else:
        # mainly for backtesting and update history (mimic behavior at the morning)
        enddatestr = (
            (pd.to_datetime(asofdate) - timedelta(days = 1))
                .tz_localize(SERVER_TIMEZONE)
                .tz_convert("UTC")
                .strftime("%Y-%m-%d")
        )
 
    startdatestr = ((pd.to_datetime(asofdate) - timedelta(days = 5))
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
            )
 

    sql = """
    CREATE TEMP TABLE temp_universe
    (
        securityid int
    )
    ON COMMIT DELETE ROWS;
    BEGIN TRANSACTION;
    """
    cursor.execute(sql)

    sql = """SELECT * FROM information_schema.tables"""
    cursor.execute(sql)
    data = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    schema = pd.DataFrame(data,columns = columns).table_schema.unique()
    for i in schema:
        if 'pg_temp' in i:
            tschema = i


    sec_list = ""
    for sid in ls_ids:
        sec_list += "INSERT INTO %s.temp_universe VALUES (%s);\n"%(tschema,sid)


    sql = f"""
    {sec_list}
    SELECT 
    c.companyid
    ,ti.tradingItemId
    ,mi.priceDate
    ,mi.priceClose
    ,mi.priceOpen
    ,mi.priceHigh
    ,mi.priceLow
    ,mi.volume
    ,mi.vwap
    
    FROM ciqCompany c
    JOIN ciqSecurity s on s.companyid = c.companyid
    JOIN ciqTradingItem ti on ti.securityId=s.securityId
    JOIN miadjprice mi on mi.tradingItemId=ti.tradingItemId
    
    WHERE c.companyId in (SELECT securityid FROM {tschema}.temp_universe) 
    AND s.primaryflag=1
    AND ti.primaryflag=1
    AND mi.priceDate <= '{enddatestr}'
    AND mi.priceDate >= '{startdatestr}'
    """
    pr = read_sql_to_df(sql, connection, cursor)   
    pr = pr.drop_duplicates(['companyid'], keep = 'last')
    pr.loc[:,'priceclose'] = pr.loc[:,'priceclose'].astype(float)
    pr.loc[:,'priceopen'] = pr.loc[:,'priceopen'].astype(float)
    pr.loc[:,'pricehigh'] = pr.loc[:,'pricehigh'].astype(float)
    pr.loc[:,'pricelow'] = pr.loc[:,'pricelow'].astype(float)
    pr.loc[:,'volume'] = pr.loc[:,'volume'].astype(float)
    return pr
  
  
def get_hist_miadj_pricing(start, end, ls_ids, connection = None):
    """
    Get a historical price data given a series of company ids, using miadjusted table 
    instead of ciqpeequity table 
    
    Args:
        start (str): '2020-05-05'
        end (str): '2020-06-06'
        ls_ids (list): list of companyid   [24937, ]
        connection (None, optional): Description
    
    Returns:
        sample ouput: 
        companyid  tradingitemid   pricedate      priceclose       priceopen       pricehigh        pricelow                 volume       vwap               divadjclose  divadjfactor
    0       24937        2590360  2020-05-05  74.39000000000  73.76500000000  75.25000000000  73.61500000000  147751200.00000000000  74.637500  73.273994703436000000000  0.9849979124
    1       24937        2590360  2020-05-06  75.15750000000  75.11500000000  75.81000000000  74.71750000000  142333760.00000000000  75.437500  74.029980601203000000000  0.9849979124
    2       24937        2590360  2020-05-07  75.93500000000  75.80500000000  76.29250000000  75.49250000000  115215040.00000000000  75.927500  74.795816478094000000000  0.9849979124
    3       24937        2590360  2020-05-08  77.53250000000  76.41000000000  77.58750000000  76.07250000000  134047960.00000000000  76.947500  76.576081355087250000000  0.9876642873

    """

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
    
    startstr = pd.to_datetime(start)
    endstr = pd.to_datetime(end)
    
    sql = f"""
    SELECT 
    c.companyid
    ,ti.tradingItemId
    ,ti.currencyid
    ,mi.priceDate
    ,mi.priceClose
    ,mi.priceOpen
    ,mi.priceHigh
    ,mi.priceLow
    ,mi.volume
    ,mi.vwap

    ,(mi.priceClose*COALESCE(daf.divAdjFactor,1)) divAdjClose
    ,COALESCE(daf.divAdjFactor,1) as divAdjFactor

    FROM ciqCompany c
    JOIN ciqSecurity s on s.companyid = c.companyid
    JOIN ciqTradingItem ti on ti.securityId=s.securityId
    JOIN miadjprice mi on mi.tradingItemId=ti.tradingItemId

    left join ciqPriceEquityDivAdjFactor daf on mi.tradingItemId=daf.tradingItemId
    and daf.fromDate<=mi.priceDate --Find dividend adjustment factor on pricing date
    and (daf.toDate is null or daf.toDate>=mi.priceDate)

    WHERE c.companyId in ({', '.join([str(id) for id in ls_ids])})  
    AND s.primaryflag=1 -- empirically makes sense to have these primary flag, lost about 0.03% data
    AND ti.primaryflag=1
    AND mi.priceDate >= '{startstr}'
    AND mi.priceDate <= '{endstr}'
    ORDER BY mi.priceDate asc;
    """
    return read_sql_to_df(sql, connection, cursor) 

def get_all_eps_estimates(cids, start, end, connection = None):
    return get_all_estimates(cids, start, end, itemid = 21634, connection = connection)


def get_all_estimates(cids, start, end, itemid = 21634, connection = None):
    """
    get historical PIT analyst & broker estimates
    :param cids: list of companyids
    :param start: start of the period
    :param end: end of the period
    :return: dataframe of estimates
    """
    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()

    startstr = (
        pd.to_datetime(start)
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
    )
    endstr = (
        pd.to_datetime(end)
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
    )

    sql = """
    CREATE TEMP TABLE temp_universe
    (
        companyid int
    )
    ON COMMIT DELETE ROWS;
    BEGIN TRANSACTION;
    """
    cursor.execute(sql)

    sql = """SELECT * FROM information_schema.tables"""
    cursor.execute(sql)
    data = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    schema = pd.DataFrame(data,columns = columns).table_schema.unique()
    for i in schema:
        if 'pg_temp' in i:
            tschema = i

    company_list = ""
    for cid in cids:
        company_list += "INSERT INTO %s.temp_universe VALUES (%s);\n"%(tschema,cid)

    sql = f"""
    {company_list}
    select EP.periodEndDate
    , C.companyName
    , C.companyId
    , EP.periodTypeId
    --- , (select EPT.periodTypeName from targetskma.ciqEstimatePeriodType EPT where EPT.periodTypeId = EP.periodTypeId) as periodTypeName
    , EP.fiscalYear
    , EP.fiscalQuarter
    --- , EB.brokerName as brokerName
    --- , EA.firstName+' '+EA.lastName as AnalystName
    --- , EA.analystObjectId
    , EDND.tradingitemid
    , EDND.estimateAnalystId
    , (select DI.dataItemName from targetskma.ciqdataitem DI where DI.dataitemid = EDND.dataitemid) as dataItemName
    --- , (select EAS.accountingStandardDescription from targetskma.ciqEstimateAccountingStd EAS where EAS.accountingStandardId = EDND.accountingStandardId) as AccountingStandard
    , (select Cu.ISOCode from targetskma.ciqCurrency Cu where Cu.currencyid = EDND.currencyid) as ISOCode
    --- , (select EST.estimateScaleName from targetskma.ciqEstimateScaleType EST where EST.estimateScaleId = EDND.estimateScaleId) as estimateScaleName
    ,EDND.dataItemValue,EDND.effectiveDate,EDND.isExcluded
	from targetskma.ciqEstimatePeriod EP
    --- link estimate period table to detailed numeric data table
    ----------------------------------------------------------
    left join targetskma.ciqCompany C
    on C.companyId = EP.companyId
    join targetskma.ciqEstimateDetailNumericData EDND
    on EDND.estimatePeriodId = EP.estimatePeriodId
    ----------------------------------------------------------
    --- left outer join targetskma.ciqEstimateBroker EB
    --- on EB.estimateBrokerId = EDND.estimateBrokerId --- left outer join must be used if you receive any of the anonymous estimates packages
    --- left outer join targetskma.ciqEstimateAnalyst EA
    --- on EA.estimateAnalystId = EDND.estimateAnalystId --- left outer join must be used if you receive any of the anonymous estimates packages
    where EP.companyId in (SELECT companyid FROM {tschema}.temp_universe)
    --- and EP.periodTypeId in (2) -- quarter
    and EDND.dataItemId = {itemid} --- in (21634, 21642) --- EPS Normalized Estimate: 21634; Revenue Estimate: 21642 
    and EDND.effectiveDate between '{startstr}' and '{endstr}'
    --- order by 4,5,6,10 
    """

    return read_sql_to_df(sql, connection, cursor) 


def get_real_estimates_with_earningsdate_appended(asofdate, cids, connection = None):
    """
    Get real time estimates with earnings date appended  

    Args:
        asofdate (str): '2021-06-16'
        cids (list): list of companyid   [24937, ]
        connection (None, optional): Description
    
    Returns:
        sample ouput: 
   fiscalyear  fiscalquarter advancedate  companyid  estimateanalystid  tradingitemid dataitemvalue       effectivedate     todate  isexcluded mostimportantdateutc
0        2021              2         NaT   78749412            -1345.0       83105693       2.40000 2021-06-02 00:25:26 2079-06-06           0           2021-07-30
1        2021              2         NaT   78749412            -1297.0       83105693       2.14000 2021-04-30 03:23:05 2079-06-06           0           2021-07-30
2        2021              2         NaT   78749412            -1159.0       83105693       2.16000 2021-04-29 22:45:07 2079-06-06           0           2021-07-30
3        2021              2         NaT   78749412             -919.0       83105693       2.11000 2021-06-03 16:01:24 2079-06-06           0           2021-07-30
4        2021              2         NaT   78749412             -817.0       83105693       2.31000 2021-04-29 21:21:15 2079-06-06           0           2021-07-30

    """
    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
    sql = f"""
    select 
    epp.fiscalYear
    ,epp.fiscalQuarter
    ,epp.advancedate
    ,epp.companyid
    , EDND.estimateAnalystId
    , EDND.tradingitemid
    , EDND.dataItemValue
    , EDND.effectiveDate
    , EDND.toDate
    , EDND.isExcluded

    from ciqEstimatePeriod epp

    join ciqEstimateDetailNumericData EDND on EDND.estimatePeriodId = epp.estimatePeriodId
                                        and EDND.toDate > '{asofdate}'
                                        and EDND.effectiveDate > '2021-01-01'

    where 1=1
    and epp.periodTypeId = 2 --Q
    and epp.companyId in ({', '.join([str(id) for id in cids])}) 
    and EDND.dataItemId = 21634 --- EPS Normalized (Detailed)

    """
    df = read_sql_to_df(sql, connection, cursor) 

    sql_earningDate = f"""
     select
        c.companyId
        ,ee.mostImportantDateUTC
        ,eer.fiscalyear
        ,eer.fiscalQuarter
        
        from ciqcompany c
        join ciqEventToObjectToEventType et on et.objectId = c.companyid and et.keyDevId in (
            select a.keyDevId from ciqEventToObjectToEventType a where a.keyDevEventTypeId in (55, 144)) 
        join ciqEvent ee on et.keyDevId=ee.keyDevId
        join ciqEventERInfo eer on eer.keydevid= ee.keydevid

        where 1=1
        and c.companyId in ({', '.join([str(id) for id in cids])}) 
        and ee.mostImportantDateUTC > '{asofdate}'

        """

    df_earningDate = read_sql_to_df(sql_earningDate, connection, cursor) 
    # print(df_earningDate.head())

    df_combined = pd.merge(df, df_earningDate,  how='inner', left_on=['fiscalyear', 'fiscalquarter', 'companyid'], right_on = ['fiscalyear', 'fiscalquarter', 'companyid'])

    # print(df_combined.head())

    return df_combined




def get_hist_mi_pricing(start, end, ls_ids, connection = None):
    """
    Get a historical price data given a series of company ids
    instead of ciqpeequity table 
    
    Args:
        start (str): '2020-05-05'
        end (str): '2020-06-06'
        ls_ids (list): list of companyid   [24937, ]
        connection (None, optional): Description
    
    Returns:
        sample ouput: 
   		companyid  tradingitemid   pricedate       priceclose        priceopen        pricehigh         pricelow                volume
	     0      25026        2590705  2021-05-14   61.46000000000   61.19000000000   62.38000000000   60.25000000000    403937.00000000000

    """

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
    
    startstr = pd.to_datetime(start)
    endstr = pd.to_datetime(end)
    
    sql = f"""
    SELECT 
    c.companyid
    ,ti.tradingItemId
    ,mi.priceDate
    ,mi.priceClose
    ,mi.priceOpen
    ,mi.priceHigh
    ,mi.priceLow
    ,mi.volume

    FROM ciqCompany c
    JOIN ciqSecurity s on s.companyid = c.companyid
    JOIN ciqTradingItem ti on ti.securityId=s.securityId
    JOIN miprice mi on mi.tradingItemId=ti.tradingItemId

    WHERE c.companyId in ({', '.join([str(id) for id in ls_ids])})  
    AND s.primaryflag=1
    AND ti.primaryflag=1
    AND mi.priceDate >= '{startstr}'
    AND mi.priceDate <= '{endstr}'
    ORDER BY mi.priceDate asc;   
    """
    return read_sql_to_df(sql, connection, cursor) 



def get_mi_pricing_ref_ti(start, end, ls_ids, connection = None):
    """
    get mi pricing with regerence of tradingitemid
    """

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
    
    startstr = pd.to_datetime(start)
    endstr = pd.to_datetime(end)
    
    sql = f"""
    SELECT 
    ti.tradingItemId
    ,mi.priceDate
    ,mi.priceClose
    ,mi.priceOpen
    ,mi.priceHigh
    ,mi.priceLow
    ,mi.volume

    FROM ciqTradingItem ti 
    JOIN miprice mi on mi.tradingItemId=ti.tradingItemId

    WHERE ti.tradingitemid in ({', '.join([str(id) for id in ls_ids])})  
    AND mi.priceDate >= '{startstr}'
    AND mi.priceDate <= '{endstr}'
    ORDER BY mi.priceDate asc;   
    """
    return read_sql_to_df(sql, connection, cursor) 




def get_all_target_price_estimates(cids, start, end, connection = None):
    return get_all_estimates(cids, start, end, itemid = 21626, connection = connection)


def get_all_ltg_estimates(cids, start, end, connection = None):
    return get_all_estimates(cids, start, end, itemid = 21629, connection = connection)



def get_detail_est_network(cids, start, end, connection = None):
    """
    get historical PIT analyst & broker estimates
    :param cids: list of companyids
    :param start: start of the period
    :param end: end of the period
    :return: dataframe of estimates
    """


    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()

    startstr = (
        pd.to_datetime(start)
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
    )
    endstr = (
        pd.to_datetime(end)
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
    )

    sql = """
    CREATE TEMP TABLE temp_universe
    (
        companyid int
    )
    ON COMMIT DELETE ROWS;
    BEGIN TRANSACTION;
    """
    cursor.execute(sql)

    sql = """SELECT * FROM information_schema.tables"""
    cursor.execute(sql)
    data = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    schema = pd.DataFrame(data,columns = columns).table_schema.unique()
    for i in schema:
        if 'pg_temp' in i:
            tschema = i

    company_list = ""
    for cid in cids:
        company_list += "INSERT INTO %s.temp_universe VALUES (%s);\n"%(tschema,cid)

    sql = f"""
    {company_list}
    select C.companyId
    , EDND.estimateAnalystId
    , count(EDND.dataItemValue) as NumEst
    from targetskma.ciqEstimatePeriod EP
    left join targetskma.ciqCompany C
    on C.companyId = EP.companyId
    join targetskma.ciqEstimateDetailNumericData EDND
    on EDND.estimatePeriodId = EP.estimatePeriodId
    where EP.companyId in (SELECT companyid FROM {tschema}.temp_universe)
    and EDND.estimateAnalystId > 0
    --- and EDND.dataItemId = 21634 --- in (21634, 21642) --- EPS Normalized Estimate: 21634; Revenue Estimate: 21642 
    and EDND.effectiveDate between '{startstr}' and '{endstr}'
    Group by C.companyId,EDND.estimateAnalystId
    """

    return read_sql_to_df(sql, connection, cursor) 

def get_portfolio_universe(pid, asofdate, connection = None):
    """
    List the current holdings of a fund (based on most recently available reporting and current prices)
    :param pid: portfolio (comnpany id) of the fund
    :return:
    """
    datestr = (
        pd.to_datetime(asofdate)
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
    )

    mc_delay_date = ((pd.to_datetime(asofdate) - timedelta(days = 10))
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
            )
 
    volstart = ((pd.to_datetime(asofdate) - timedelta(days = 360))
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
            )


    sql = f"""
            DROP TABLE IF EXISTS preProcessed;
            CREATE TEMP TABLE preProcessed AS
            SELECT pl.ownerObjectId
            ,c.companyName
            ,c.companyid
            ,s.securityId
            ,s.securityName
            ,ti.tradingitemID
            ,pl.holdingDate
            ,pl.fromDate
            ,pl.weightingDate
            ,pl.sharesHeld
            ,pl.optionsHeld
            ,pl.percentOfPortfolio
            ,pl.percentofSharesOutstanding
            ,idm.identifiervalue as isin
            ,idm.activeflag as isinflag
            FROM targetskma.ciqOwnPortfolioLatest pl
            INNER JOIN targetskma.ciqSecurity s
            ON pl.securityId = s.securityID
            INNER JOIN targetskma.ciqCompany c
            ON s.companyId = c.companyId
            INNER JOIN ciqTradingItem AS ti ON ti.securityId=s.securityId
            INNER JOIN ciqsecurityidentifier AS idm ON s.securityId=idm.securityId
            WHERE pl.ownerObjectId = '{pid}'
            AND ti.primaryflag=1 AND ti.currencyid=160
            AND s.primaryflag=1
            ORDER BY pl.percentOfPortfolio DESC;

            SELECT 
            preProcessed.*,
            mcm.marketcap,
            adv.volume
            FROM
            preProcessed
            INNER JOIN 
            (
            	SELECT 
            		mc.companyID, 
            		mc.marketcap
                FROM ciqMarketCap AS mc
                INNER JOIN 
            	(
            	SELECT companyID, MAX(pricingdate) AS MaxDate
            	FROM ciqMarketCap
            		WHERE 
                        pricingdate BETWEEN '{mc_delay_date}' AND '{datestr}'
            		AND
                        companyID IN (SELECT companyID FROM preProcessed)
            	GROUP BY companyID
                ) AS tm 
            	ON mc.companyID = tm.companyID AND mc.pricingdate = tm.MaxDate
            	-- WHERE mc.marketcap >= {MKTCAP_THRES} 
            ) AS mcm 	
            ON mcm.companyID=preProcessed.companyID

            INNER JOIN 
            (
            	SELECT 
            		tradingItemId, 
            		AVG(volume * priceClose) AS volume
            	FROM targetskma.ciqPriceEquity
            	WHERE 
            		pricingDate BETWEEN '{volstart}' AND '{datestr}'
            	AND 
            		tradingItemId IN (SELECT tradingitemID FROM preProcessed)
            	GROUP BY tradingItemId
            ) AS adv 
            ON preProcessed.tradingItemId=adv.tradingItemId
            AND adv.volume >= {ADV_THRES}
        """
    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
    
    
    return read_sql_to_df(sql, connection, cursor) 



def get_hist_earnings_release_dates(ls_ids, fromdate, todate, sortby = 'projectedEarningDatesUTC', connection = None):
    """
        get the earning dates for a list of companies, the earning call dates are used as fallback
        notice, for example, 3102672 is of bad coverage

        note: I could not find a flag stating whether a event is for quarterly, semi-ann, annually
    
    """
    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
    
    if sortby == 'projectedEarningDatesUTC':
        sort_values = 'mostimportantdateutc'
    elif sortby == 'enterDBDatesET':
        sort_values = 'entereddate'

    sql = f'''
            SELECT 

            ee.keydevid
            ,co.companyid
            ,e.headline
            ,e.announceddateutc -- UTC
            ,e.mostimportantdateutc AS futureearningdatesutc-- UTC
            ,e.entereddate AS entereddateET -- ET
            ,et.keyDevEventTypeName
            ,et.keyDevEventTypeId
            ,co.companyname

            FROM CiqEvent AS e
            JOIN ciqEventToObjectToEventType AS ee ON ee.keyDevId = e.keyDevId
            JOIN ciqEventType AS et ON et.keyDevEventTypeId = ee.keyDevEventTypeId
            JOIN ciqCompany AS co ON ee.objectId = co.companyId

            WHERE 
            1 = 1
            AND e.{sort_values} >= '{fromdate}' 
            AND '{todate}' >= e.{sort_values}
            AND co.companyId in ({', '.join([str(id) for id in ls_ids])})
            AND et.keyDevEventTypeId IN (55, 144) 

    ''' # event table only has 55, 144 earnings release date 
        # do not have 28
        # goes back to history as well cover future

        # Mostly, 55 Earing Release Date event is at least one month earlier than the Earning Date
    return read_sql_to_df(sql, connection, cursor)  


def get_earnings_announcement_dates(ls_ids, fromdate, todate, connection = None):
    """
    """
    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()

    sql = f'''
            SELECT 

            ee.keydevid
            ,co.companyid
            ,e.headline
            ,e.announceddate -- UTC
            ,e.mostimportantdateutc -- UTC
            ,e.entereddate AS entereddateET -- ET
            ,et.keyDevEventTypeName
            ,et.keyDevEventTypeId
            ,co.companyname

            FROM Ciqkeydev AS e
            JOIN ciqKeyDevToObjectToEventType AS ee ON ee.keyDevId = e.keyDevId
            JOIN ciqkeydevEventType AS et ON et.keyDevEventTypeId = ee.keyDevEventTypeId
            JOIN ciqCompany AS co ON ee.objectId = co.companyId

            WHERE 
            1 = 1
            AND e.mostimportantdateutc >= '{fromdate}' 
            AND '{todate}' >= e.mostimportantdateutc
            AND co.companyId in ({', '.join([str(id) for id in ls_ids])})
            AND et.keyDevEventTypeId in (28) 

    ''' # event table only has 55, 144 earnings release date 
        # do not have 28
        # goes back to history as well cover future

        # Mostly, 55 Earing Release Date event is at least one month earlier than the Earning Date
    



    return read_sql_to_df(sql, connection, cursor)  


def get_keydates(ls_ids, keydevids, fromdate, todate, connection = None):
    """
    """
    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()

    sql = f'''
            SELECT 

            ee.keydevid
            ,co.companyid
            ,e.headline
            ,e.announceddate -- UTC
            ,e.mostimportantdateutc -- UTC
            ,e.entereddate AS entereddateET -- ET
            ,et.keyDevEventTypeName
            ,et.keyDevEventTypeId
            ,co.companyname

            FROM Ciqkeydev AS e
            JOIN ciqKeyDevToObjectToEventType AS ee ON ee.keyDevId = e.keyDevId
            JOIN ciqkeydevEventType AS et ON et.keyDevEventTypeId = ee.keyDevEventTypeId
            JOIN ciqCompany AS co ON ee.objectId = co.companyId

            WHERE 
            1 = 1
            AND e.mostimportantdateutc >= '{fromdate}' 
            AND '{todate}' >= e.mostimportantdateutc
            AND co.companyId in ({', '.join([str(id) for id in ls_ids])})
            AND et.keyDevEventTypeId in ({', '.join([str(id) for id in keydevids])})

    ''' 
    return read_sql_to_df(sql, connection, cursor)  



def get_afl_factor_express(date, ls_ids, factorids, connection = None):

    datestr = (
        pd.to_datetime(date)
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
    )

    sql_afl = f"""
            select 
            dly.factorvalue
            , dly.factorid
            , gvk.objectId
            , dly.asofdate
            , d.securityid
            , gvk.gvkey
            , gvk.iid
            from ciqAFValuedailyna dly

            join ciqgvkeyiid gvk 
            on gvk.gvkey = dly.gvkey
            and gvk.iid = dly.iid
            and gvk.activeflag = 1 

            JOIN ciqTradingItem d ON gvk.objectId = d.tradingItemId AND d.currencyid = 160 AND d.primaryflag = 1
            JOIN ciqExchange e ON d.exchangeId = e.exchangeId AND e.countryId = 213 --US, listed in US exchange


            where dly.factorId in ({', '.join([str(factor_id) for factor_id in factorids])})
            and asOfDate = '{datestr}'
            """

    sql_cid = f"""
            select s.securityid, c.companyid, c.companyname
            from ciqSecurity s 

            JOIN ciqCompany c ON c.companyid = s.companyid AND c.countryid = 213 --US, the company is based in the US
            AND c.companyId in ({', '.join([str(id) for id in ls_ids])})

            where s.primaryflag = 1
    """


    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
                
    afl = read_sql_to_df(sql_afl, connection, cursor)
    # print(afl.query('factorid == 11').dropna())
    # assert False
    cid = read_sql_to_df(sql_cid, connection, cursor)

    return afl.merge(cid, left_on='securityid', right_on='securityid')



def get_afl_factor_monthly_pit(date, factorids, ls_ids, connection = None):

    datestr = (
        pd.to_datetime(date)
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
    )

    sql_afl = f"""
            select 
            dly.factorvalue
            , dly.factorid
            , gvk.objectId
            , dly.asofdate
            , d.securityid
            , gvk.gvkey
            , gvk.iid
            from ciqafvaluemonthlyna dly

            join ciqgvkeyiid gvk 
            on gvk.gvkey = dly.gvkey
            and gvk.iid = dly.iid
            and gvk.activeflag = 1 

            JOIN ciqTradingItem d ON gvk.objectId = d.tradingItemId AND d.currencyid = 160 AND d.primaryflag = 1
            JOIN ciqExchange e ON d.exchangeId = e.exchangeId AND e.countryId = 213 --US, listed in US exchange


            where dly.factorId in ({', '.join([str(factor_id) for factor_id in factorids])})
            and asOfDate = '{datestr}'
            """

    sql_cid = f"""
            select s.securityid, c.companyid, c.companyname
            from ciqSecurity s 

            JOIN ciqCompany c ON c.companyid = s.companyid AND c.countryid = 213 --US, the company is based in the US
            AND c.companyId in ({', '.join([str(id) for id in ls_ids])})
    
    """


    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
                
    afl = read_sql_to_df(sql_afl, connection, cursor)
    # print(afl.query('factorid == 11').dropna())
    # assert False
    cid = read_sql_to_df(sql_cid, connection, cursor)

    return afl.merge(cid, left_on='securityid', right_on='securityid')


def get_afl_factor_monthly_period(begin, end, factorids, connection = None):

    begin_datestr = (
        pd.to_datetime(begin)
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
    )

    end_datestr = (
        pd.to_datetime(end)
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
    )

    sql_afl = f"""
            select 
            dly.factorvalue
            , dly.factorid
            , gvk.objectId
            , dly.asofdate
            , d.securityid
            , gvk.gvkey
            , gvk.iid
            from ciqafvaluemonthlyna dly

            join ciqgvkeyiid gvk 
            on gvk.gvkey = dly.gvkey
            and gvk.iid = dly.iid
            and gvk.activeflag = 1 

            JOIN ciqTradingItem d ON gvk.objectId = d.tradingItemId AND d.currencyid = 160 AND d.primaryflag = 1
            JOIN ciqExchange e ON d.exchangeId = e.exchangeId AND e.countryId = 213 --US, listed in US exchange


            where dly.factorId in ({', '.join([str(factor_id) for factor_id in factorids])})
            and asOfDate >= '{begin_datestr}'
            and asOfDate <= '{end_datestr}'

            """

    sql_cid = f"""
            select s.securityid, c.companyid, c.companyname
            from ciqSecurity s 

            JOIN ciqCompany c ON c.companyid = s.companyid AND c.countryid = 213 --US, the company is based in the US
    """


    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
                
    afl = read_sql_to_df(sql_afl, connection, cursor)
    # print(afl.query('factorid == 11').dropna())
    # assert False
    cid = read_sql_to_df(sql_cid, connection, cursor)

    return afl.merge(cid, left_on='securityid', right_on='securityid')



def get_live_mipricing(asofdate, ls_ids, connection = None):
    """
    """

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
    
    asofdate = pd.to_datetime(asofdate)
    
    sql = f"""
    SELECT 
    c.companyid
    , c.companyname
    ,ti.tradingItemId
    ,mi.priceDate
    ,mi.priceClose
    ,mi.priceOpen
    ,mi.priceHigh
    ,mi.priceLow
    ,mi.volume

    FROM ciqCompany c
    JOIN ciqSecurity s on s.companyid = c.companyid
    JOIN ciqTradingItem ti on ti.securityId=s.securityId
    JOIN miprice mi on mi.tradingItemId=ti.tradingItemId

    WHERE c.companyId in ({', '.join([str(id) for id in ls_ids])})  
    AND s.primaryflag=1
    AND ti.primaryflag=1
    AND mi.priceDate = '{asofdate}'

    """
    return read_sql_to_df(sql, connection, cursor) 



def get_company_industryid(ls_ids, connection = None):
    """
    """

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
        
    sql = f"""
    SELECT 
    c.companyid
    , c.simpleindustryid

    FROM ciqCompany c

    WHERE c.companyId in ({', '.join([str(id) for id in ls_ids])})  

    """
    return read_sql_to_df(sql, connection, cursor) 


def get_companyid_from_isin(isin_ids, connection = None):
    """
    """

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
        
    sql = f"""
	SELECT 
    *
	FROM
	ciqsecurityidentifier AS idm

    WHERE 
    idm.identifiervalue in ({', '.join([str(id) for id in isin_ids])})  

    """
    return read_sql_to_df(sql, connection, cursor) 


def get_isin_from_secid(sec_ids, connection = None):
    """
    """

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
        
    sql = f"""
	SELECT 
    *
	FROM
	ciqsecurityidentifier AS idm

    WHERE 
    idm.securityid in ({', '.join([str(id) for id in sec_ids])})  

    """
    return read_sql_to_df(sql, connection, cursor) 


##
# mar. 22. 2022
def get_ref_gvkeyiid(connection = None):
    '''
    get information about gvkeyiid s, including tradingitemid and related companyid
    
    Args:
        gvkeyiids (list): 
        connection (None, optional): connection = get_connection(DBINFO)
    
    Returns:
            gvkiid  relatedcompanyid    objectid symbolstartdate symbolenddate
    574   21041891             18527     2585899             NaT           NaT
    1948  18425401             18833   128978901             NaT    2019-09-04
    3191  18425401             18833  1762730483      2022-01-03           NaT
    3193  18425401             18833   634694368      2019-09-05    2022-01-02
    1413  10547501             18965   117445254             NaT           NaT
    ...        ...               ...         ...             ...           ...
    3202  10608601         586199515   649840326      2019-02-08           NaT
    3158  33546601         644297441   644341184             NaT           NaT
    3185  18959402         666048874   666198975             NaT           NaT
    3197  19056001         705098315  1762432731      2022-01-19           NaT
    2926  13860102        1683184724   429912206             NaT           NaT
    '''

    sql = f"""
        select 
        *
        from 
        ciqgvkeyiid
    """

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
    
    return read_sql_to_df(sql, connection, cursor)


def get_ref_cik(ls_ids, get_cik = True, connection = None):
    '''
    get information about cik s, including tradingitemid and related companyid
    
    Args:
        ls_ids (list): 
        connection (None, optional): connection = get_connection(DBINFO)
    
    Returns:

    '''
    if get_cik:
        sql = f"""
            select 
            
            symbolvalue
            ,relatedcompanyid as companyid

            from 
            ciqsymbol

            where

            symboltypeid = 21
            and
            activeflag = 1
            and
            relatedcompanyid in ({', '.join([str(id) for id in ls_ids])})  
        """
    else:
        sql = f"""
            select 
            
            symbolvalue
            ,relatedcompanyid as companyid

            from 
            ciqsymbol

            where

            symboltypeid = 21
            and
            activeflag = 1
            and
            CAST(symbolvalue as int) in ({', '.join([str(id) for id in ls_ids])})  
        """

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
    
    return read_sql_to_df(sql, connection, cursor)


# mar. 21. 2022
def get_universe(date, mktcap_thres, adv_thres, connection = None):
    '''
    get the idmaps from company id to corresponding isin
    :param date: universe as of the date (incl.)
    :return:
    Extract tradeable companies and its ISINs:
        given that the companies meets MKTCAP_THRES --
                most recent market cap if exists within last 10 days
            and (ATV)average trading volume ADV_THRES --
                ATV is calculated using mean(last 360days)
    
    Args:
        date (str): '2020-03-03' (incl.)
        connection (None, optional): connection = get_connection(DBINFO)
    
    Returns:
        pd.DataFrame: sample:
        df:               isin  securityid      securityname  tradingitemid  companyid                      companyname     marketcap                 volume
        0     US0003602069     2585892      Common Stock        2585893     320500                       AAON, Inc.   2926.540470   7015116.145080645161
        1     US0003611052     2585894      Common Stock        2585895     168154                        AAR Corp.   1254.929933   9664062.786411290323
        2     US0009571003     2586015      Common Stock        2586016     250079      ABM Industries Incorporated   2265.596955  12838018.320443548387
        3     US0247531058     2586015      Common Stock        2586016     250079      ABM Industries Incorporated   2265.596955  12838018.320443548387
        4     US0258701061     2586085      Common Stock        2586086     250178               Aflac Incorporated  31306.821931     162019261.84407258
    '''

    datestr = (
        pd.to_datetime(date)
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
    )
 
    volstart = ((pd.to_datetime(date) - timedelta(days = 360))
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
            )

    sql = f"""
	DROP TABLE IF EXISTS preProcessed;

	CREATE TEMP TABLE preProcessed AS
	SELECT 

	s.securityid
	, s.securityname
	, ti.tradingitemID
	, cpny.companyID
	, cpny.companyname
    , cpny.companytypeid
    , cpny.simpleindustryid
    , cpny.incorporationCountryId
    , cpny.countryid
    , ti.exchangeid
    , ti.currencyid
    , ec.exchangeName

	FROM ciqCompany AS cpny
	INNER JOIN ciqSecurity AS s ON cpny.companyID=s.companyID
	INNER JOIN ciqTradingItem AS ti ON ti.securityId=s.securityId
	INNER JOIN ciqExchange AS ec ON ti.exchangeid = ec.exchangeid

	WHERE 
	s.securitySubTypeId in (1,2) 
    AND 
    s.primaryflag=1
	AND
	ti.primaryflag=1 AND ti.currencyid=160
	AND
	cpny.countryid=213
    AND
    cpny.incorporationcountryid=213
    AND
    cpny.companytypeid != 13;

    -- the main part 
	SELECT 
	preProcessed.*,
	mcm.marketcap
	,adv.volume

	FROM 
	preProcessed 

	INNER JOIN 
	(
		SELECT 
			mc.companyID, 
			mc.marketcap
	    FROM ciqMarketCap AS mc
	    INNER JOIN 
		(
		SELECT companyID, pricingdate 
		FROM ciqMarketCap
			WHERE 
				pricingdate = '{datestr}'
			AND
				companyID IN (SELECT companyID FROM preProcessed)
	    ) AS tm 
		ON mc.companyID = tm.companyID AND mc.pricingdate = tm.pricingdate
		WHERE mc.marketcap >= {mktcap_thres} 
	) AS mcm 	
	ON mcm.companyID=preProcessed.companyID

	INNER JOIN 
	(
		SELECT 
			tradingItemId, 
			AVG(volume * priceClose) AS volume
		FROM miadjprice
		WHERE 
			priceDate BETWEEN '{volstart}' AND '{datestr}'
		AND 
			tradingItemId IN (SELECT tradingitemID FROM preProcessed)
		GROUP BY tradingItemId
	) AS adv 
	ON preProcessed.tradingItemId=adv.tradingItemId
	AND adv.volume >= {adv_thres}
    """

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
    
    return read_sql_to_df(sql, connection, cursor)


# mar. 22. 2022
def get_tradingitem_detail(tradingitems, connection = None):
    '''
    get information about gvkeyiid s, including tradingitemid and related companyid
    
    Args:
        gvkeyiids (list): 
        connection (None, optional): connection = get_connection(DBINFO)
    
    Returns:
            gvkiid  relatedcompanyid    objectid symbolstartdate symbolenddate
    574   21041891             18527     2585899             NaT           NaT
    1948  18425401             18833   128978901             NaT    2019-09-04
    3191  18425401             18833  1762730483      2022-01-03           NaT
    3193  18425401             18833   634694368      2019-09-05    2022-01-02
    1413  10547501             18965   117445254             NaT           NaT
    ...        ...               ...         ...             ...           ...
    3202  10608601         586199515   649840326      2019-02-08           NaT
    3158  33546601         644297441   644341184             NaT           NaT
    3185  18959402         666048874   666198975             NaT           NaT
    3197  19056001         705098315  1762432731      2022-01-19           NaT
    2926  13860102        1683184724   429912206             NaT           NaT
    '''

    sql = f"""
        select 

        ti.tradingitemid
        , ti.exchangeid
        , ec.importancelevel
        , ec.exchangesymbol
        , ti.tickersymbol
        , ti.currencyid

        from 
        ciqtradingitem ti
        
        INNER JOIN ciqExchange AS ec ON ti.exchangeid = ec.exchangeid
        INNER JOIN ciqSecurity AS s ON ti.securityid = s.securityid
        INNER JOIN ciqCompanyid AS c ON s.companyid = c.companyid


        WHERE ti.tradingitemid in ({', '.join([str(id) for id in tradingitems])})  
        -- and ti.currencyid = 160
        -- and ec.countryid = 213

    """

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
    
    return read_sql_to_df(sql, connection, cursor)


# mar. 24. 2022
def get_companyid_from_securityid(securityids, connection = None):

    sql = f"""
        select 

        c.companyid
        , s.securityid

        from 
        ciqcompany c
        
        INNER JOIN ciqsecurity s ON c.companyid = s.companyid


        WHERE s.securityid in ({', '.join([str(id) for id in securityids])})  

    """

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
    
    return read_sql_to_df(sql, connection, cursor)


# apr. 8. 2022
def get_industryid(companyids, connection = None):

    sql = f"""
        select 

        c.companyid
        , c.simpleindustryid
        , s.simpleindustrydescription

        from 
        ciqcompany c
        
        INNER JOIN ciqsimpleindustry s ON s.simpleindustryid = c.simpleindustryid


        WHERE c.companyid in ({', '.join([str(id) for id in companyids])})  

    """

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
    
    return read_sql_to_df(sql, connection, cursor)


def get_target_price(asofdate, ls_ids, dataitemids, connection = None):
    """
    """
    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()

    sql = f'''

    select 
    EC.tradingItemId
    , ED.dataItemValue
    , EP.companyId
    , ED.dataItemId
    , ED.currencyid

    from ciqEstimatePeriod EP

    --- link the core estimate table to data table
    -----------------------------------------------------------
    join ciqEstimateConsensus EC
    on EC.estimatePeriodId = EP.estimatePeriodId
    
    join ciqEstimateNumericData ED
    on ED.estimateConsensusId = EC.estimateConsensusId
    
    --- applying only the primary security and trading item
    -----------------------------------------------------------
    join ciqTradingItem TI
    on TI.tradingItemId = EC.tradingItemId
    and TI.primaryFlag = 1
    join ciqSecurity S
    on S.securityId = TI.securityId
    and S.primaryFlag = 1

    where EP.companyId in ({', '.join([str(id) for id in ls_ids])})
    and ED.dataItemId in ({', '.join([str(id) for id in dataitemids])})
    and '{asofdate}' between ED.effectiveDate and ED.toDate
    ''' 
    return read_sql_to_df(sql, connection, cursor)  


def get_target_price_ref_ti(asofdate, ls_ids, dataitemids, connection = None):
    """
    """
    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()

    sql = f'''

    select 
    EC.tradingItemId
    , ED.dataItemValue
    , EP.companyId
    , ED.dataItemId
    , ED.currencyid
    , ED.effectiveDate
    , ED.toDate

    from ciqEstimatePeriod EP

    --- link the core estimate table to data table
    -----------------------------------------------------------
    join ciqEstimateConsensus EC
    on EC.estimatePeriodId = EP.estimatePeriodId
    
    join ciqEstimateNumericData ED
    on ED.estimateConsensusId = EC.estimateConsensusId
    
    --- applying only the primary security and trading item
    -----------------------------------------------------------
    join ciqTradingItem TI
    on TI.tradingItemId = EC.tradingItemId
    where TI.tradingItemId in ({', '.join([str(id) for id in ls_ids])})
    and ED.dataItemId in ({', '.join([str(id) for id in dataitemids])})
    and '{asofdate}' between ED.effectiveDate and ED.toDate
    ''' 
    return read_sql_to_df(sql, connection, cursor)  


def get_hist_revenue_estimate(ls_ids, dataitemids, connection = None):
    """
    """
    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()

    sql = f'''

    select 
    ED.asofdate
    ,ED.estimateConsensusId 
    , ED.dataItemValue
    , EP.companyId
    , ED.dataItemId

    from ciqEstimatePeriod EP

    --- link the core estimate table to data table
    -----------------------------------------------------------
    join ciqEstimateConsensus EC
    on EC.estimatePeriodId = EP.estimatePeriodId
    
    join ciqestimateanalysisdata ED
    on ED.estimateConsensusId = EC.estimateConsensusId

    where EP.companyId in ({', '.join([str(id) for id in ls_ids])})
    and ED.dataItemId in ({', '.join([str(id) for id in dataitemids])})
    ''' 
    return read_sql_to_df(sql, connection, cursor)  


def get_hist_target_price(ls_ids, dataitemids, connection = None):
    """
    """
    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()

    sql = f'''

    select 
    EC.tradingItemId
    , ED.dataItemValue
    , EP.companyId
    , ED.dataItemId
    , ED.effectiveDate
    , ED.toDate

    from ciqEstimatePeriod EP

    --- link the core estimate table to data table
    -----------------------------------------------------------
    join ciqEstimateConsensus EC
    on EC.estimatePeriodId = EP.estimatePeriodId
    
    join ciqEstimateNumericData ED
    on ED.estimateConsensusId = EC.estimateConsensusId
    
    --- applying only the primary security and trading item
    -----------------------------------------------------------
    join ciqTradingItem TI
    on TI.tradingItemId = EC.tradingItemId
    and TI.primaryFlag = 1
    join ciqSecurity S
    on S.securityId = TI.securityId
    and S.primaryFlag = 1

    where EP.companyId in ({', '.join([str(id) for id in ls_ids])})
    and ED.dataItemId in ({', '.join([str(id) for id in dataitemids])})
    and ED.effectiveDate >= '2008-01-01'
    ''' 
    return read_sql_to_df(sql, connection, cursor)  




def get_live_with_hist_target_price(startdate, enddate, ls_ids, dataitemids, connection = None):
    """
    """
    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()

    sql = f'''

    select 
    EC.tradingItemId
    , ED.dataItemValue
    , EP.companyId
    , ED.dataItemId
    , ED.effectiveDate
    , ED.toDate

    from ciqEstimatePeriod EP

    --- link the core estimate table to data table
    -----------------------------------------------------------
    join ciqEstimateConsensus EC
    on EC.estimatePeriodId = EP.estimatePeriodId
    
    join ciqEstimateNumericData ED
    on ED.estimateConsensusId = EC.estimateConsensusId
    
    --- applying only the primary security and trading item
    -----------------------------------------------------------
    join ciqTradingItem TI
    on TI.tradingItemId = EC.tradingItemId
    and TI.primaryFlag = 1
    join ciqSecurity S
    on S.securityId = TI.securityId
    and S.primaryFlag = 1

    where EP.companyId in ({', '.join([str(id) for id in ls_ids])})
    and ED.dataItemId in ({', '.join([str(id) for id in dataitemids])})
    and ED.toDate >= '{startdate}'
    and ED.effectiveDate <= '{enddate}'
    ''' 
    return read_sql_to_df(sql, connection, cursor)  



# july 18 2022
def get_hist_estimate_from_analysisdata(datestart, dateend, ls_ids, dataitemids, connection = None):

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()

    sql = f'''

    select 
    ED.asofdate
    ,ED.estimateConsensusId 
    , ED.dataItemValue
    , EP.companyId
    , ED.dataItemId

    from ciqEstimatePeriod EP

    --- link the core estimate table to data table
    -----------------------------------------------------------
    join ciqEstimateConsensus EC
    on EC.estimatePeriodId = EP.estimatePeriodId
    
    join ciqestimateanalysisdata ED
    on ED.estimateConsensusId = EC.estimateConsensusId

    where EP.companyId in ({', '.join([str(id) for id in ls_ids])})
    and ED.dataItemId in ({', '.join([str(id) for id in dataitemids])})
    and asofdate BETWEEN '{datestart}' AND '{dateend}'

    ''' 
    return read_sql_to_df(sql, connection, cursor)  


def get_cur_estimate_from_analysisdata(todate, startdate, ls_ids, dataitemids, connection = None):

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()

    sql = f'''

    select 
    ED.asofdate
    ,ED.estimateConsensusId 
    , ED.dataItemValue
    , EP.companyId
    , ED.dataItemId

    from ciqEstimatePeriod EP

    --- link the core estimate table to data table
    -----------------------------------------------------------
    join ciqEstimateConsensus EC
    on EC.estimatePeriodId = EP.estimatePeriodId
    
    join ciqestimateanalysisdata ED
    on ED.estimateConsensusId = EC.estimateConsensusId

    where EP.companyId in ({', '.join([str(id) for id in ls_ids])})
    and ED.dataItemId in ({', '.join([str(id) for id in dataitemids])})
    and asofdate <= '{todate}'
    and asofdate >= '{startdate}'

    ''' 
    return read_sql_to_df(sql, connection, cursor)  


def get_hist_estimate_from_numericdata(datestart, dateend, ls_ids, dataitemids, connection = None):

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()

    sql = f'''

    select 
    ED.effectivedate
    ,ED.todate
    , ED.estimateConsensusId 
    , ED.dataItemValue
    , EP.companyId
    , ED.dataItemId
    , EP.periodtypeid

    from ciqEstimatePeriod EP

    --- link the core estimate table to data table
    -----------------------------------------------------------
    join ciqEstimateConsensus EC
    on EC.estimatePeriodId = EP.estimatePeriodId
    
    join ciqestimatenumericdata ED
    on ED.estimateConsensusId = EC.estimateConsensusId

    where EP.companyId in ({', '.join([str(id) for id in ls_ids])})
    and ED.dataItemId in ({', '.join([str(id) for id in dataitemids])})
    and ED.toDate >= '{datestart}'
    and ED.effectiveDate <= '{dateend}'
    -- and EP.periodtypeid = 2
    ''' 
    return read_sql_to_df(sql, connection, cursor)  


def get_cur_estimate_from_numericdata(asofdate, ls_ids, dataitemids, connection = None):

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()

    sql = f'''

    select 
    ED.effectivedate
    ,ED.todate
    , ED.estimateConsensusId 
    , ED.dataItemValue
    , EP.companyId
    , ED.dataItemId

    from ciqEstimatePeriod EP

    --- link the core estimate table to data table
    -----------------------------------------------------------
    join ciqEstimateConsensus EC
    on EC.estimatePeriodId = EP.estimatePeriodId
    
    join ciqestimatenumericdata ED
    on ED.estimateConsensusId = EC.estimateConsensusId

    where EP.companyId in ({', '.join([str(id) for id in ls_ids])})
    and ED.dataItemId in ({', '.join([str(id) for id in dataitemids])})
    and ED.toDate >= '{asofdate}'
    and ED.effectiveDate <= '{asofdate}'

    ''' 
    return read_sql_to_df(sql, connection, cursor)  


def get_companyname(ls_ids, connection = None):

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()

    sql = f"""
            SELECT 
            c.companyId,
            c.companyname 
            
            FROM ciqcompany c 

            WHERE    c.companyId in ({', '.join([str(id) for id in ls_ids])})
            
        """
    
    return read_sql_to_df(sql, connection, cursor)   


# get stock split 
def get_stocksplit(connection = None):

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()

    sql = f"""
        select * from ciqsplit 
        where splittypeid = 12 
        and exdate >= '2022-07-30'
        and exdate <= '2022-08-16';
            
        """
    
    return read_sql_to_df(sql, connection, cursor)   


    
#######
def test(ls_ids, ls_dataitemid, connection = None):


    """
    @author: zheng
    reWrite from function get_PIT_fundamental
    reference: CIQ Premium Financials Sample Appendix B, P140

    get fundamentals finacial from CIQ PIT premium financials
    CIQ PIT database has a backfill mechanism causing some instance dates to be much later than filing date for earlier years
    but normally the maximum processing time is less than a month, so assume any lag greater than a month is a backfill data


    select a.dataItemName, a.dataItemId from ciqDataItem a inner join ciqFinCollectionData b
    on a.dataItemId=b.dataItemId where a.dataItemName like '%split%'
    Args:
            companyid (list): list of company id e.g. [6631173, 429233, 33414482, 262353065, 1600081]
            ls_dataitemid (list): dataitemid: dataitemid in table ciqDataItem, e.g.
                                            1100 is common shares outstanding (adjusted)
                                            4379 is normalized basic EPS (comparable to consensus)
                                            112987 is book value
            connection (None, optional): Description

    Returns:
        sample output:
                   companyid periodenddate filingdate formtype   calendarquarter  calendaryear  dataitemid  dataitemvalue        
0                 262353065    2020-12-31 2021-03-02     10-K      4          2020        1100       11.624717
1                 6631173    2021-01-02 2021-03-02     10-K        4          2020        1100      64.252859 
2                1600081    2021-01-02 2021-03-02     10-K          4          2020        1100      94.854336
3                429233    2020-12-31 2021-03-11     10-K         4          2020        1100    27.175305 

    """    

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()

    sql = f"""
            SELECT 
            fp.companyId, 
            fi.periodEndDate,
            fi.filingDate,
            fi.formtype,
            fp.calendarQuarter, 
            fp.calendarYear,
            fd.dataItemId,
            fd.dataItemValue,
            ciqdataitem.dataitemname
            
            FROM ciqFinPeriod fp 
            join ciqFinInstance fi on fi.financialPeriodId = fp.financialPeriodId 
            join ciqFinInstanceToCollection ic on ic.financialInstanceId = fi.financialInstanceId 
            join ciqFinCollectionData fd on fd.financialCollectionId = ic.financialCollectionId 
            join ciqdataitem on ciqdataitem.dataitemid = fd.dataItemId

            WHERE    fp.companyId in ({', '.join([str(id) for id in ls_ids])})
            AND  fi.periodEndDate > '2022-01-01'
            AND  fi.periodEndDate > '2022-01-01'
            
        """
    
    return read_sql_to_df(sql, connection, cursor)           


# owner
def get_live_holder_of_co(connection = None):

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()

    sql = f"""
    SELECT 

    CASE 
        WHEN a.ownerObjectId=c.companyId then c.companyName
        WHEN a.ownerObjectId=p.personId then CONCAT(p.firstname, ' ', p.lastName)
    END ownerName
    , sharesHeld
    , percentOfPortfolio
    , percentOfSharesOutstanding SharesOut
    , sharesChanged
    , percentSharesChanged sharesChanged
    , rankSharesHeld
    , rankSharesBought
    , rankSharesSold
    , ownedcompanyid
    
    from ciqowncompanyholding a 
    
    left join ciqCompany c
    on c.companyid=a.ownerobjectid
    
    left join ciqPerson p on 
    a.ownerObjectId=p.personId
    
    where
    a.latestflag = 1 
    and 
    ownedcompanyid = 24739 
    
    order by a.sharesHeld desc
    """
    
    return read_sql_to_df(sql, connection, cursor)   


def get_live_type_of_holder_of_co(connection = None):

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()

    sql = f"""
    select 
    c.companyName
    , b.ownerTypeName
    , b.ownerTypeId
    , a.sharesHeld as sharesHeld
    , round(a.sharesHeld * 100.0 / sum(a.sharesHeld) over (),2) as Total

    from ciqOwnHoldingCompanySummary a 
    
    join ciqOwnerType b
    on a.ownerTypeId=b.ownerTypeId
    
    join ciqCompany c 
    on c.companyId=a.ownedCompanyId
    
    where ownedCompanyId=24739
    order by sharesHeld desc
    """
    
    return read_sql_to_df(sql, connection, cursor)   


def get_netinsidertrading(connection = None):

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()

    sql = f"""
    select c.companyName
    , a.securityId
    , a.dataDate
    , a.netShares1Mth
    , a.netShares3Mths

    from ciqOwnInsiderAggregate a 
    
    join ciqSecurity b
    on a.securityId=b.securityId
    
    join ciqCompany c
    on c.companyId=b.companyId
    
    where c.companyId in (24937,20765463,18749,32012,21835,29096) 
    and b.primaryflag=1
    """
    
    return read_sql_to_df(sql, connection, cursor) 

def get_hist_holder_of_co(connection = None):

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()

    sql = f"""
    SELECT 

    CASE 
        WHEN a.ownerObjectId=c.companyId then c.companyName
        WHEN a.ownerObjectId=p.personId then CONCAT(p.firstname, ' ', p.lastName)
    END ownerName
    , sharesHeld
    , percentOfPortfolio
    , percentOfSharesOutstanding SharesOut
    , sharesChanged
    , percentSharesChanged sharesChanged
    , rankSharesHeld
    , rankSharesBought
    , rankSharesSold
    
    from ciqowncompanyholding a 

    join ciqownholdingperiod b
    on a.periodid=b.periodid
    
    left join ciqcompany c
    on c.companyid=a.ownerobjectid
    
    left join ciqPerson p on 
    a.ownerObjectId=p.personId
    
    where
    a.periodid=75
    and 
    ownedcompanyid=38043467 --Target Corp
    
    order by a.sharesHeld desc
        """
    
    return read_sql_to_df(sql, connection, cursor)   



    
def get_cur_miadj_pricing_tradingitem(asofdate, ls_ids, connection = None):

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()

    enddatestr = (
        pd.to_datetime(asofdate)
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
    )
 
    startdatestr = ((pd.to_datetime(asofdate) - timedelta(days = 5))
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
            )

    sql = f"""
    SELECT 
    ti.tradingItemId
    ,mi.priceDate
    ,mi.priceClose
    ,mi.priceOpen
    ,mi.priceHigh
    ,mi.priceLow
    ,mi.volume
    ,mi.vwap
    
    FROM ciqTradingItem ti 
    JOIN miadjprice mi on mi.tradingItemId=ti.tradingItemId
    
    WHERE ti.tradingItemId in ({', '.join([str(id) for id in ls_ids])})
    AND ti.primaryflag=1
    AND mi.priceDate <= '{enddatestr}'
    AND mi.priceDate >= '{startdatestr}'
    """
    pr = read_sql_to_df(sql, connection, cursor)  
    pr.sort_values(['pricedate'], inplace = True)
    pr.drop_duplicates(['tradingitemid'], keep = 'last', inplace = True) 
    pr.loc[:,'priceclose'] = pr.loc[:,'priceclose'].astype(float)
    pr.loc[:,'priceopen'] = pr.loc[:,'priceopen'].astype(float)
    pr.loc[:,'pricehigh'] = pr.loc[:,'pricehigh'].astype(float)
    pr.loc[:,'pricelow'] = pr.loc[:,'pricelow'].astype(float)
    pr.loc[:,'volume'] = pr.loc[:,'volume'].astype(float)
    return pr


def get_afl_factor_intl(date, ls_ids, factorids, connection = None):

    datestr = (
        pd.to_datetime(date)
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
    )

    sql_afl = f"""
            select 
            dly.factorvalue
            , dly.factorid
            , gvk.objectId
            , dly.asofdate
            , gvk.gvkey
            , gvk.iid
            from ciqAFValuedailyintl dly

            join ciqgvkeyiid gvk 
            on gvk.gvkey = dly.gvkey
            and gvk.iid = dly.iid
            and gvk.activeflag = 1 

            where 
            gvk.objectId in ({', '.join([str(id) for id in ls_ids])})
            and dly.factorId in ({', '.join([str(factor_id) for factor_id in factorids])})
            and asOfDate = '{datestr}'
            """


    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
                
    return read_sql_to_df(sql_afl, connection, cursor)


#### global
def get_universe_global(date, mktcap_thres, adv_thres, countrycode, currencyid, connection = None):
    '''
    get the idmaps from company id to corresponding isin
    :param date: universe as of the date (incl.)
    :return:
    Extract tradeable companies and its ISINs:
        given that the companies meets MKTCAP_THRES --
                most recent market cap if exists within last 10 days
            and (ATV)average trading volume ADV_THRES --
                ATV is calculated using mean(last 360days)
    
    Args:
        date (str): '2020-03-03' (incl.)
        connection (None, optional): connection = get_connection(DBINFO)
    
    Returns:
        pd.DataFrame: sample:
        df:               isin  securityid      securityname  tradingitemid  companyid                      companyname     marketcap                 volume
        0     US0003602069     2585892      Common Stock        2585893     320500                       AAON, Inc.   2926.540470   7015116.145080645161
        1     US0003611052     2585894      Common Stock        2585895     168154                        AAR Corp.   1254.929933   9664062.786411290323
        2     US0009571003     2586015      Common Stock        2586016     250079      ABM Industries Incorporated   2265.596955  12838018.320443548387
        3     US0247531058     2586015      Common Stock        2586016     250079      ABM Industries Incorporated   2265.596955  12838018.320443548387
        4     US0258701061     2586085      Common Stock        2586086     250178               Aflac Incorporated  31306.821931     162019261.84407258
    '''

    datestr = (
        pd.to_datetime(date)
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
    )
 
    volstart = ((pd.to_datetime(date) - timedelta(days = 360))
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
            )

    sql = f"""
	DROP TABLE IF EXISTS preProcessed;

	CREATE TEMP TABLE preProcessed AS
	SELECT 

	s.securityid
	, s.securityname
	, ti.tradingitemID
	, cpny.companyID
	, cpny.companyname
    , cpny.companytypeid
    , cpny.simpleindustryid
    , cpny.incorporationCountryId
    , cpny.countryid
    , ti.exchangeid
    , ti.currencyid
    , ec.exchangeName

	FROM ciqCompany AS cpny
	INNER JOIN ciqSecurity AS s ON cpny.companyID=s.companyID
	INNER JOIN ciqTradingItem AS ti ON ti.securityId=s.securityId
	INNER JOIN ciqExchange AS ec ON ti.exchangeid = ec.exchangeid

	WHERE 
	s.securitySubTypeId in (1,2, 3, 22) 
    AND 
    s.primaryflag=1
	AND
	ti.primaryflag=1 AND ti.currencyid={currencyid}
	AND
	ec.countryid in ({', '.join([str(countryid) for countryid in countrycode])})
    AND
    -- not including companie that are essentially ETFs or funds
    cpny.companytypeid != 13 
    ;

    -- the main part 
	SELECT 
	preProcessed.*,
	mcm.marketcap
	,adv.volume

	FROM 
	preProcessed 

	INNER JOIN 
	(
		SELECT 
			mc.companyID, 
			mc.marketcap
	    FROM ciqMarketCap AS mc
	    INNER JOIN 
		(
		SELECT companyID, pricingdate 
		FROM ciqMarketCap
			WHERE 
				pricingdate = '{datestr}'
			AND
				companyID IN (SELECT companyID FROM preProcessed)
	    ) AS tm 
		ON mc.companyID = tm.companyID AND mc.pricingdate = tm.pricingdate
		WHERE mc.marketcap >= {mktcap_thres} 
	) AS mcm 	
	ON mcm.companyID=preProcessed.companyID

	INNER JOIN 
	(
		SELECT 
			tradingItemId, 
			AVG(volume * priceClose) AS volume
		FROM miadjprice
		WHERE 
			priceDate BETWEEN '{volstart}' AND '{datestr}'
		AND 
			tradingItemId IN (SELECT tradingitemID FROM preProcessed)
		GROUP BY tradingItemId
	) AS adv 
	ON preProcessed.tradingItemId=adv.tradingItemId
	AND adv.volume >= {adv_thres}
    """

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
    
    return read_sql_to_df(sql, connection, cursor)


# fx rate  
def get_cur_fxrate(asofdate, ls_ids, connection = None):

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()

    sql = f"""
    SELECT 

    fxrate.currencyid
    , fxrate.pricedate
    , fxrate.priceclose
    
    FROM ciqexchangerate fxrate 
    
    WHERE fxrate.currencyid in ({', '.join([str(id) for id in ls_ids])})
    AND fxrate.priceDate = '{asofdate}'
    AND fxrate.latestsnapflag = 1 
    """
    pr = read_sql_to_df(sql, connection, cursor)  
    pr.loc[:,'priceclose'] = pr.loc[:,'priceclose'].astype(float)
    pr = pr.rename(columns = {'priceclose':'fxrate'})

    return pr

def get_hist_fxrate(fromdate, connection = None):

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()

    sql = f"""
    SELECT 

    fxrate.currencyid
    , fxrate.pricedate
    , fxrate.priceclose
    
    FROM ciqexchangerate fxrate 
    
    WHERE fxrate.priceDate >= '{fromdate}'
    AND fxrate.latestsnapflag = 1 
    """
    pr = read_sql_to_df(sql, connection, cursor)  
    pr.loc[:,'priceclose'] = pr.loc[:,'priceclose'].astype(float)
    pr = pr.rename(columns = {'priceclose':'fxrate'})

    return pr


def get_pit_universe_global(connection = None):

    sql = f"""

	SELECT 

	s.securityid
	, s.securitySubTypeId
	, ti.tradingitemID
	, cpny.companyID
	, cpny.companyname
    , cpny.companytypeid
    , cpny.companystatustypeid
    , cpny.simpleindustryid
    , cpny.incorporationCountryId
    , cpny.countryid
    , ti.exchangeid
    -- , ti.tradingitemstatusid
    , ti.tickersymbol
    , ti.currencyid
    , ec.exchangeName
    , currency.isocode
    -- , geo.country

	FROM ciqCompany AS cpny
	INNER JOIN ciqSecurity AS s ON cpny.companyID=s.companyID
	INNER JOIN ciqTradingItem AS ti ON ti.securityId=s.securityId
	INNER JOIN ciqExchange AS ec ON ti.exchangeid = ec.exchangeid
    LEFT JOIN ciqCurrency as currency on currency.currencyid = ti.currencyid
    -- LEFT JOIN ciqCountryGeo as geo on cpny.countryid = geo.countryid

	WHERE 
    -- 1 --> common shares, 2 --> adr, 
    -- 3 --> preference share, 22 --> special kind shares
	s.securitySubTypeId in (1, 2, 3, 22) 
    AND 
    s.primaryflag=1
	AND
	ti.primaryflag=1 
    AND
    -- include only 4 --> public company, 1 --> public investment firm
    -- this works only with pit current profile (for historical universe, need to allow other companytype like 5 --> private company)
    cpny.companytypeid in (1, 4) 
    AND
    -- 1 --> operating
    -- 2 --> operating subsidiary
    cpny.companystatustypeid in (1, 2)
    AND
    -- tradingitemstatusid 15 --> active
    -- this works only with pit current profile (for historical universe, need to allow other tradingitemstatus)
    ti.tradingitemstatusid = 15
    ;
    """

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
    
    return read_sql_to_df(sql, connection, cursor)


def get_pit_universe_global_hist(connection = None):
    
    sql = f"""

	SELECT 

	s.securityid
	, s.securitySubTypeId
	, ti.tradingitemID
	, cpny.companyID
	, cpny.companyname
    , cpny.companytypeid
    , cpny.companystatustypeid
    , cpny.simpleindustryid
    , cpny.incorporationCountryId
    , cpny.countryid
    , ti.exchangeid
    -- , ti.tradingitemstatusid
    , ti.tickersymbol
    , ti.currencyid
    , ec.exchangeName
    , currency.isocode
    , geo.country

	FROM ciqCompany AS cpny
	INNER JOIN ciqSecurity AS s ON cpny.companyID=s.companyID
	INNER JOIN ciqTradingItem AS ti ON ti.securityId=s.securityId
	INNER JOIN ciqExchange AS ec ON ti.exchangeid = ec.exchangeid
    LEFT JOIN ciqCurrency as currency on currency.currencyid = ti.currencyid
    LEFT JOIN ciqCountryGeo as geo on cpny.countryid = geo.countryid

	WHERE 
    -- 1 --> common shares, 2 --> adr, 
    -- 3 --> preference share, 22 --> special kind shares
	s.securitySubTypeId in (1, 2, 3, 22) 
    AND 
    s.primaryflag=1
	AND
	ti.primaryflag=1
    AND
    -- do not include only 13 --> ETFs and funds, 1 --> public investment firm
    cpny.companytypeid not in (1, 13) 
    ;
    """

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
    
    return read_sql_to_df(sql, connection, cursor)

def vol_filter(tids, date, connection = None):

    datestr = (
        pd.to_datetime(date)
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
    )
 
    volstart = ((pd.to_datetime(date) - timedelta(days = 365*2))
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
            )
    
    sql = f"""
		SELECT 
			tradingItemId,  
            COUNT(*) AS daycount,
			AVG(volume * priceClose) AS volume
		FROM miadjprice
		WHERE 
			priceDate BETWEEN '{volstart}' AND '{datestr}'
		AND 
			tradingItemId IN ({', '.join([str(id) for id in tids])})
		GROUP BY tradingItemId
    """
    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
    
    return read_sql_to_df(sql, connection, cursor)


def get_cur_mc_global(cids, date, lookback_date = 5, connection = None):

    datestr = (
        pd.to_datetime(date)
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
    )
 
    datestart = ((pd.to_datetime(date) - timedelta(days = lookback_date))
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
            )
    if type(cids) is str:
        sql = f"""
            SELECT companyID, pricingdate, marketcap, tev
            FROM ciqMarketCap
                WHERE 
                    pricingdate between '{datestart}' and '{datestr}'
                    """
    else:
        sql = f"""
            SELECT companyID, pricingdate, marketcap, tev
            FROM ciqMarketCap
                WHERE 
                    pricingdate between '{datestart}' and '{datestr}'
                AND
                    companyID IN ({', '.join([str(id) for id in cids])})
        """

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
    
    return read_sql_to_df(sql, connection, cursor)




def get_countrygeo_map(ls_ids, connection = None):
    sql = f"""
		SELECT country, countryid
		FROM ciqCountrygeo
			WHERE 
				countryid IN ({', '.join([str(id) for id in ls_ids])});
    """

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
    
    return read_sql_to_df(sql, connection, cursor)




def test1(ls_ids, dataitemids, asofdate, forecastyear_start, forecastyear_end, connection = None):
    datestr = (
        pd.to_datetime(asofdate)
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
    )
    sql = f"""
        select '{datestr}' as observeDate
        , EP.companyid
        , EP.periodTypeId
        , EP.fiscalYear
        , EP.calendarYear
        , ED.dataitemid
        , ED.currencyId
        , ED.dataItemValue
        , ED.effectiveDate
        , ED.toDate

        from ciqEstimatePeriod EP
        --- link the core estimate table to data table
        --------------------------------------------------------------
        join ciqEstimateConsensus EC 
        on EC.estimatePeriodId = EP.estimatePeriodId
        join ciqEstimateNumericData ED
        on ED.estimateConsensusId = EC.estimateConsensusId
        --------------------------------------------------------------
        --- use RelConst to limit EstimatePeriod to the forward 3 periods
        --------------------------------------------------------------
        -- join ciqEstimatePeriodRelConst EPRC
        -- on EPRC.estimatePeriodId = EP.estimatePeriodId
        -- and EPRC.periodTypeId = EP.periodTypeId
        -- and EPRC.relativeConstant - 1000 between 1 and 3
        --------------------------------------------------------------
        where EP.companyId IN ({', '.join([str(id) for id in ls_ids])})
        and EP.periodTypeId = 1 -- annual 
        and ED.dataItemId in ({', '.join([str(id) for id in dataitemids])})
        and '{datestr}' between ED.effectiveDate and ED.toDate

        and EP.calendarYear >= {forecastyear_start}
        and EP.calendarYear <= {forecastyear_end}

        order by 4
    """

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
    
    return read_sql_to_df(sql, connection, cursor)



def get_estimates_hist(ls_ids, dataitemids, fromdate, periodtype = 1, connection = None):
    datestr = (
        pd.to_datetime(fromdate)
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
    )
    sql = f"""
        select 
          EP.companyid
        , EP.periodTypeId
        , EP.fiscalYear
        , EP.calendarYear
        , ED.dataitemid
        , ED.currencyId
        , ED.dataItemValue
        , ED.effectiveDate
        , ED.toDate

        from ciqEstimatePeriod EP
        --- link the core estimate table to data table
        --------------------------------------------------------------
        join ciqEstimateConsensus EC 
        on EC.estimatePeriodId = EP.estimatePeriodId
        join ciqEstimateNumericData ED
        on ED.estimateConsensusId = EC.estimateConsensusId
        --------------------------------------------------------------
        where EP.companyId IN ({', '.join([str(id) for id in ls_ids])})
        and EP.periodTypeId = {periodtype} -- annual 
        and ED.dataItemId in ({', '.join([str(id) for id in dataitemids])})
        and ED.effectiveDate >  '{datestr}' 
    """

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
    
    return read_sql_to_df(sql, connection, cursor)


def get_estimates_cur_q_ref_co(ls_ids, dataitemids, asofdate, connection = None):
    datestr = (
        pd.to_datetime(asofdate)
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d %H:%M:%S")
    )
    datestart = ((pd.to_datetime(asofdate) - timedelta(days = 90))
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
            )

    sql = f"""
        select '{datestr}' as observeDate
        , EP.*
        , ED.dataitemid
        , ED.currencyId
        , ED.dataItemValue
        , ED.effectiveDate
        , ED.toDate
        , EC.tradingitemid
        , ED.estimatescaleid

        from ciqEstimatePeriod EP
        --- link the core estimate table to data table
        --------------------------------------------------------------
        join ciqEstimateConsensus EC 
        on EC.estimatePeriodId = EP.estimatePeriodId
        join ciqEstimateNumericData ED
        on ED.estimateConsensusId = EC.estimateConsensusId
        --------------------------------------------------------------
        where EP.companyId IN ({', '.join([str(id) for id in ls_ids])})
        and EP.periodTypeId = 2 -- Quarter 
        and ED.dataItemId in ({', '.join([str(id) for id in dataitemids])})
        and '{datestr}' between ED.effectiveDate and ED.toDate
        and EP.advancedate is null
        and EP.periodenddate > '{datestart}'

        order by 4
    """
    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
    
    return read_sql_to_df(sql, connection, cursor)

def get_act_q_ref_co(ls_ids, dataitemids, asofdate, connection = None):
    datestr = (
        pd.to_datetime(asofdate)
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d %H:%M:%S")
    )
    datestart = ((pd.to_datetime(asofdate) - timedelta(days = 360*5))
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
            )

    sql = f"""
        select '{datestr}' as observeDate
        , EP.*
        , ED.dataitemid
        , ED.currencyId
        , ED.dataItemValue
        , ED.effectiveDate
        , ED.toDate
        , EC.tradingitemid
        , ED.estimatescaleid

        from ciqEstimatePeriod EP
        --- link the core estimate table to data table
        --------------------------------------------------------------
        join ciqEstimateConsensus EC 
        on EC.estimatePeriodId = EP.estimatePeriodId
        join ciqEstimateNumericData ED
        on ED.estimateConsensusId = EC.estimateConsensusId
        --------------------------------------------------------------
        where EP.companyId IN ({', '.join([str(id) for id in ls_ids])})
        and EP.periodTypeId = 2 -- Quarter 
        and ED.dataItemId in ({', '.join([str(id) for id in dataitemids])})
        and EP.periodenddate > '{datestart}'
        and ED.toDate > '2030-01-01'

        order by 4
    """
    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
    
    return read_sql_to_df(sql, connection, cursor)


def get_hist_act_q_ref_co(ls_ids, dataitemids, startdate, enddate, connection = None):

    sql = f"""
        select 
        EP.*
        , ED.dataitemid
        , ED.currencyId
        , ED.dataItemValue
        , ED.effectiveDate
        , ED.toDate
        , EC.tradingitemid
        , ED.estimatescaleid

        from ciqEstimatePeriod EP
        --- link the core estimate table to data table
        --------------------------------------------------------------
        join ciqEstimateConsensus EC 
        on EC.estimatePeriodId = EP.estimatePeriodId
        join ciqEstimateNumericData ED
        on ED.estimateConsensusId = EC.estimateConsensusId
        --------------------------------------------------------------
        where EP.companyId IN ({', '.join([str(id) for id in ls_ids])})
        and EP.periodTypeId = 2 -- Quarter 
        and ED.dataItemId in ({', '.join([str(id) for id in dataitemids])})
        and EP.periodenddate >= '{startdate}'
        and EP.periodenddate <= '{enddate}'
        and ED.toDate > '2030-01-01'

        order by 4
    """
    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
    
    return read_sql_to_df(sql, connection, cursor)

def get_estimates_cur_q(ls_ids, dataitemids, asofdate, connection = None):
    datestr = (
        pd.to_datetime(asofdate)
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d %H:%M:%S")
    )
    datestart = ((pd.to_datetime(asofdate) - timedelta(days = 90))
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
            )

    sql = f"""
        select '{datestr}' as observeDate
        , EP.*
        , ED.dataitemid
        , ED.currencyId
        , ED.dataItemValue
        , ED.effectiveDate
        , ED.toDate
        , EC.tradingitemid

        from ciqEstimatePeriod EP
        --- link the core estimate table to data table
        --------------------------------------------------------------
        join ciqEstimateConsensus EC 
        on EC.estimatePeriodId = EP.estimatePeriodId
        join ciqEstimateNumericData ED
        on ED.estimateConsensusId = EC.estimateConsensusId
        --------------------------------------------------------------
        where EC.tradingitemid IN ({', '.join([str(id) for id in ls_ids])})
        and EP.periodTypeId = 2 -- Quarter 
        and ED.dataItemId in ({', '.join([str(id) for id in dataitemids])})
        and '{datestr}' between ED.effectiveDate and ED.toDate
        and EP.advancedate is null
        and EP.periodenddate > '{datestart}'

        order by 4
    """
    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
    
    return read_sql_to_df(sql, connection, cursor)


def get_estimates_q(ls_ids, dataitemids, asofdate, connection = None):
    datestr = (
        pd.to_datetime(asofdate)
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d %H:%M:%S")
    )
    datestart = ((pd.to_datetime(asofdate) - timedelta(days = 360*6))
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
            )

    sql = f"""
        select '{datestr}' as observeDate
        , EP.*
        , ED.dataitemid
        , ED.currencyId
        , ED.dataItemValue
        , ED.effectiveDate
        , ED.toDate
        , EC.tradingitemid

        from ciqEstimatePeriod EP
        --- link the core estimate table to data table
        --------------------------------------------------------------
        join ciqEstimateConsensus EC 
        on EC.estimatePeriodId = EP.estimatePeriodId
        join ciqEstimateNumericData ED
        on ED.estimateConsensusId = EC.estimateConsensusId
        --------------------------------------------------------------
        where EC.tradingitemid IN ({', '.join([str(id) for id in ls_ids])})
        and EP.periodTypeId = 2 -- Quarter 
        and ED.dataItemId in ({', '.join([str(id) for id in dataitemids])})
        and EP.periodenddate > '{datestart}'

        order by 4
    """
    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
    
    return read_sql_to_df(sql, connection, cursor)


def get_estimates_hist_q_ref_ti(ls_ids, dataitemids, datestart, tp = False, connection = None):
    datestart = (pd.to_datetime(datestart)
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
            )

    if tp:
        cond =f"""and ED.effectiveDate > '{datestart}'"""
    else:
        cond =f"""and EP.periodenddate > '{datestart}'"""

    if tp:
        periodtype = " "
    else:
        periodtype = "and EP.periodTypeId = 2 -- Quarter"
    sql = f"""
        select
        EP.*
        , ED.dataitemid
        , ED.currencyId
        , ED.dataItemValue
        , ED.effectiveDate
        , ED.toDate
        , EC.tradingitemid

        from ciqEstimatePeriod EP
        --- link the core estimate table to data table
        --------------------------------------------------------------
        join ciqEstimateConsensus EC 
        on EC.estimatePeriodId = EP.estimatePeriodId
        join ciqEstimateNumericData ED
        on ED.estimateConsensusId = EC.estimateConsensusId
        --------------------------------------------------------------
        where EC.tradingitemid IN ({', '.join([str(id) for id in ls_ids])})
        {periodtype}
        and ED.dataItemId in ({', '.join([str(id) for id in dataitemids])})
        {cond}

        order by 4
    """
    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
    
    return read_sql_to_df(sql, connection, cursor)



# a query from cid --> gvkeyiid 
def ref_cid_gvkeyiid(ls_ids, connection = None):

    sql = f"""
            select 
            gvk.objectId
            , d.securityid
            , gvk.gvkey
            , gvk.iid
            , s.companyid

            from ciqgvkeyiid gvk 
            

            JOIN ciqTradingItem d ON gvk.objectId = d.tradingItemId AND d.currencyid = 160 AND d.primaryflag = 1
            JOIN ciqSecurity s ON d.securityid = s.securityid
            JOIN ciqCompany c ON c.companyid = s.companyid AND c.countryid = 213

            where s.primaryflag = 1
            AND c.companyId in ({', '.join([str(id) for id in ls_ids])})
            and gvk.activeflag = 1 
            """

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
                
    return read_sql_to_df(sql, connection, cursor)



def get_transcript_ref_earliest_new(ls_ids, startdate, enddate=None, connection = None):
    """
    Get historical reference table for a list of companyid with asscoiated transcriptid from given a date range (from startdate to enddate)

    Args:
        ls_ids (list): list of company id, e.g. [6631173, 429233, 33414482, 262353065, 1600081]
        startdate (str): '2021-03-03'
        enddate (str, optional): '2021-03-30'
        connection (None, optional): Description
    
    Returns:
       sample output
    df:      transcriptid transcriptcreationdateutc  companyid                   companyname   keydevid  ...  fiscalquarter delayreasontypeid delayreasontypename iscancelledflag delayreasonnotes
        0         2228812       2021-03-03 00:15:52    6631173               B&G Foods, Inc.  704677961  ...              4               NaN                None             NaN             None
        1         2228824       2021-03-03 00:37:01     429233       TransMedics Group, Inc.  704570793  ...              4               NaN                None             NaN             None
        2         2228846       2021-03-03 01:03:12   33414482            Veeva Systems Inc.  704483765  ...              4               NaN                None             NaN             None
        3         2228848       2021-03-03 01:05:38  262353065                 Vectrus, Inc.  704552829  ...              4               NaN                None             NaN             None
        4         2228851       2021-03-03 01:10:43    1600081  Grocery Outlet Holding Corp.  704570277  ...              4               NaN                None             NaN             None

    """
    
    startdatestr = (
        pd.to_datetime(startdate)
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
    )
    
    if enddate is None:
        enddate = datetime.now()
        
    enddatestr = (
        pd.to_datetime(enddate)
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
    )
    
    sql = f"""
        SELECT DISTINCT a.transcriptId FROM targetskma.ciqTranscript a,
        (
        SELECT keyDevId,Min(transcriptCreationDateUTC) as minTm FROM targetskma.ciqTranscript
        WHERE transcriptCollectionTypeId!=7
        AND transcriptCreationDateUTC >= '{startdatestr}'
        AND transcriptCreationDateUTC <= '{enddatestr}'   
        GROUP BY keyDevId
        ) mtm
        WHERE a.keyDevId = mtm.keyDevId AND a.transcriptCreationDateUTC = mtm.minTm
            """       
    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
    
    mtm = read_sql_to_df(sql, connection, cursor)['transcriptid'].values

    sql = f"""
            SELECT t.transcriptId, t.transcriptCreationDateUTC, ete.objectId companyid, t.keyDevId, t.transcriptCollectionTypeId,
            e.mostImportantDateUTC as EarningsCallDateUTC, e.announcedDateUTC,
            eb.fiscalyear, eb.fiscalquarter
            FROM targetskma.ciqTranscript t
            JOIN targetskma.ciqEvent e ON e.keyDevId = t.keyDevId
            JOIN targetskma.ciqEventToObjectToEventType ete ON ete.keyDevId = t.keyDevId
            JOIN targetskma.ciqEventType et ON et.keyDevEventTypeId = ete.keyDevEventTypeId
            JOIN targetskma.ciqeventcallbasicinfo eb on eb.keyDevId = t.keyDevId
            WHERE et.keyDevEventTypeId='48' --Earnings Calls
            AND t.transcriptId in ({', '.join([str(id) for id in mtm])}) 
            AND ete.objectId in ({', '.join([str(id) for id in ls_ids])})         
            ORDER BY t.transcriptCreationDateUTC asc;
            """   
    
    return read_sql_to_df(sql, connection, cursor)   


### new
def earnings_on_the_date(ls_ids, fys, connection = None):
    sql = f"""
    select et.objectId as CompanyID, e.keyDevId, e.mostImportantDateUTC as EarningsDate, em.marketIndicatorTypeName, e.headline, er.fiscalyear, er.fiscalquarter, er.calendaryear
    from ciqevent e
    join ciqEventToObjectToEventType et on et.keyDevId=e.keyDevId
    join ciqEventERInfo er on e.keyDevId=er.keyDevId
    join ciqEventMarketIndicatorType em on em.marketIndicatorTypeId=er.marketIndicatorTypeId
    where 1=1
    and et.objectId in ({', '.join([str(id) for id in ls_ids])})
    and er.fiscalyear in ({', '.join([str(fy) for fy in fys])})
    """

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
                
    return read_sql_to_df(sql, connection, cursor)


def earnings_given_keydevid(ls_ids, connection = None):
    sql = f"""
    select et.objectId as CompanyID, e.keyDevId as linkedkeydevid, e.mostImportantDateUTC as EarningsDate, em.marketIndicatorTypeName, e.headline, er.fiscalyear, er.fiscalquarter, er.calendaryear
    from ciqevent e
    join ciqEventToObjectToEventType et on et.keyDevId=e.keyDevId
    join ciqEventERInfo er on e.keyDevId=er.keyDevId
    join ciqEventMarketIndicatorType em on em.marketIndicatorTypeId=er.marketIndicatorTypeId
    where 1=1
    and e.keyDevId in ({', '.join([str(id) for id in ls_ids])})
    """

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
                
    return read_sql_to_df(sql, connection, cursor)


def eventtoevent(keydevids, connection = None):
    sql = f"""
        select * from ciqeventtoevent ete
        where ete.keydevid in ({', '.join([str(id) for id in keydevids])})
        """

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
                
    return read_sql_to_df(sql, connection, cursor)

def get_epsestimatediff_ref_co(ls_ids, dataitemids, asofdate, connection = None):
    datestr = (
        pd.to_datetime(asofdate)
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d %H:%M:%S")
    )
    datestart = ((pd.to_datetime(asofdate) - timedelta(days = 360*5))
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
            )

    sql = f"""
        select '{datestr}' as observeDate
        , EP.*
        , ED.dataitemid
        , ED.currencyId
        , ED.dataItemValue
        , ED.asofdate
        , EC.tradingitemid
        , ED.scaleid

        from ciqEstimatePeriod EP
        --- link the core estimate table to data table
        --------------------------------------------------------------
        join ciqEstimateConsensus EC 
        on EC.estimatePeriodId = EP.estimatePeriodId
        join ciqEstimateanalysisdata ED
        on ED.estimateConsensusId = EC.estimateConsensusId
        --------------------------------------------------------------
        where EP.companyId IN ({', '.join([str(id) for id in ls_ids])})
        and EP.periodTypeId = 2 -- Quarter 
        and ED.dataItemId in ({', '.join([str(id) for id in dataitemids])})
        and ED.asofdate > '{datestart}'
        and ED.asofdate <= '{datestr}'
        order by 4
    """
    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
    
    return read_sql_to_df(sql, connection, cursor)

def get_hist_epsestimatediff_ref_co(ls_ids, dataitemids, startdate, enddate, connection = None):

    sql = f"""
        select EP.*
        , ED.dataitemid
        , ED.currencyId
        , ED.dataItemValue
        , ED.asofdate
        , EC.tradingitemid
        , ED.scaleid

        from ciqEstimatePeriod EP
        --- link the core estimate table to data table
        --------------------------------------------------------------
        join ciqEstimateConsensus EC 
        on EC.estimatePeriodId = EP.estimatePeriodId
        join ciqEstimateanalysisdata ED
        on ED.estimateConsensusId = EC.estimateConsensusId
        --------------------------------------------------------------
        where EP.companyId IN ({', '.join([str(id) for id in ls_ids])})
        and EP.periodTypeId = 2 -- Quarter 
        and ED.dataItemId in ({', '.join([str(id) for id in dataitemids])})
        and ED.asofdate > '{startdate}'
        and ED.asofdate <= '{enddate}'
        order by 4
    """
    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
    
    return read_sql_to_df(sql, connection, cursor)


def get_guidances(ls_ids, dataitemids, asofdate, connection = None):
    datestr = (
        pd.to_datetime(asofdate)
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d %H:%M:%S")
    )
    datestart = ((pd.to_datetime(asofdate) - timedelta(days = 360*5))
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
            )

    sql = f"""
        select '{datestr}' as observeDate
        , EP.*
        , ED.dataitemid
        , ED.currencyId
        , ED.dataItemValue
        , ED.effectiveDate
        , ED.toDate
        , EC.tradingitemid
        , ED.estimatescaleid

        from ciqEstimatePeriod EP
        --- link the core estimate table to data table
        --------------------------------------------------------------
        join ciqEstimateConsensus EC 
        on EC.estimatePeriodId = EP.estimatePeriodId
        join ciqEstimateNumericData ED
        on ED.estimateConsensusId = EC.estimateConsensusId
        --------------------------------------------------------------
        where EP.companyId IN ({', '.join([str(id) for id in ls_ids])})
        and EP.periodTypeId = 2 -- Quarter 
        and ED.dataItemId in ({', '.join([str(id) for id in dataitemids])})
        and EP.periodenddate > '{datestart}'

        order by 4
    """
    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
    
    return read_sql_to_df(sql, connection, cursor)


# aug. 8 2024
def get_all_us_universe(connection = None):
    '''
    get all us companies
    :param date: universe as of the date (incl.)
    :return:
    '''


    sql = f"""
	SELECT 

	s.securityid
	, s.securityname
	, ti.tradingitemID
	, cpny.companyID
	, cpny.companyname
    , cpny.companytypeid
    , cpny.simpleindustryid
    , cpny.incorporationCountryId
    , cpny.countryid
    , ti.exchangeid
    , ti.currencyid
    , ec.exchangeName

	FROM ciqCompany AS cpny
	INNER JOIN ciqSecurity AS s ON cpny.companyID=s.companyID
	INNER JOIN ciqTradingItem AS ti ON ti.securityId=s.securityId
	INNER JOIN ciqExchange AS ec ON ti.exchangeid = ec.exchangeid

	WHERE 
	s.securitySubTypeId in (1,2) 
    AND 
    s.primaryflag=1
	AND
	ti.primaryflag=1 AND ti.currencyid=160
	AND
	cpny.countryid=213
    AND
    cpny.incorporationcountryid=213
    AND
    cpny.companytypeid != 13;
    """

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
    
    return read_sql_to_df(sql, connection, cursor)


def get_all_transcript(ls_ids, connection = None):
    """
    Get historical reference table for a list of companyid with asscoiated transcriptid from given a date range (from startdate to enddate)

    Args:
        ls_ids (list): list of company id, e.g. [6631173, 429233, 33414482, 262353065, 1600081]
        startdate (str): '2021-03-03'
        enddate (str, optional): '2021-03-30'
        connection (None, optional): Description
    
    Returns:
       sample output
    df:      transcriptid transcriptcreationdateutc  companyid                   companyname   keydevid  ...  fiscalquarter delayreasontypeid delayreasontypename iscancelledflag delayreasonnotes
        0         2228812       2021-03-03 00:15:52    6631173               B&G Foods, Inc.  704677961  ...              4               NaN                None             NaN             None
        1         2228824       2021-03-03 00:37:01     429233       TransMedics Group, Inc.  704570793  ...              4               NaN                None             NaN             None
        2         2228846       2021-03-03 01:03:12   33414482            Veeva Systems Inc.  704483765  ...              4               NaN                None             NaN             None
        3         2228848       2021-03-03 01:05:38  262353065                 Vectrus, Inc.  704552829  ...              4               NaN                None             NaN             None
        4         2228851       2021-03-03 01:10:43    1600081  Grocery Outlet Holding Corp.  704570277  ...              4               NaN                None             NaN             None

    """
    
    sql = f"""
            SELECT t.transcriptId, t.transcriptCreationDateUTC, ete.objectId companyid, t.keyDevId, t.transcriptCollectionTypeId,
            e.mostImportantDateUTC as EarningsCallDateUTC, e.announcedDateUTC,
            eb.fiscalyear, eb.fiscalquarter
            FROM targetskma.ciqTranscript t
            JOIN targetskma.ciqEvent e ON e.keyDevId = t.keyDevId
            JOIN targetskma.ciqEventToObjectToEventType ete ON ete.keyDevId = t.keyDevId
            JOIN targetskma.ciqEventType et ON et.keyDevEventTypeId = ete.keyDevEventTypeId
            JOIN targetskma.ciqeventcallbasicinfo eb on eb.keyDevId = t.keyDevId
            WHERE et.keyDevEventTypeId='48' --Earnings Calls
            AND ete.objectId in ({', '.join([str(id) for id in ls_ids])})         
            ORDER BY t.transcriptCreationDateUTC asc;
            """  
    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor() 
    df = read_sql_to_df(sql, connection, cursor)  
    et_ref = df.sort_values(['keydevid', 'transcriptcreationdateutc']).drop_duplicates('keydevid', keep='last') # get the max id, that is with latest transcriptcreationdateutc
    return et_ref