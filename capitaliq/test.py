import pandas as pd 
import databaseManager
from databaseManager import get_traded_isin_company
from cfg import SERVER_TIMEZONE,DBINFO
from cfg import ADV_THRES, MKTCAP_THRES
import json
import psycopg2
from datetime import datetime, timedelta


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
        password=u["pwd"])
    return db

###################################################################################
def get_price_vol(date, ls_ids, connection = None):

    datestr = (
        pd.to_datetime(date)
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
    )

    sql = f"""
        SELECT c.companyid, c.companyname
        -- ,pe.pricingDate
        -- ,pe.priceClose
        -- ,pe.priceOpen
        -- ,pe.priceHigh
        -- ,pe.priceLow
        -- ,pe.volume
        -- ,(pe.priceClose*COALESCE(daf.divAdjFactor,1)) divAdjPrice
        -- ,COALESCE(daf.divAdjFactor,1) as divAdjFactor
        from targetskma.ciqCompany c
        -- join targetskma.ciqSecurity s on c.companyid=s.companyid 
        -- join targetskma.ciqTradingItem ti on ti.securityId=s.securityId
        -- join targetskma.ciqPriceEquity pe on pe.tradingItemId=ti.tradingItemId
        -- left join targetskma.ciqPriceEquityDivAdjFactor daf on pe.tradingItemId=daf.tradingItemId
        -- and daf.fromDate<=pe.pricingDate --Find dividend adjustment factor on pricing date
        -- and (daf.toDate is null or daf.toDate>=pe.pricingDate)
        
        WHERE c.companyId in ({', '.join([str(id) for id in ls_ids])})  
        -- and s.primaryflag=1
        -- and ti.primaryflag=1
        -- and pe.pricingDate>=''
        -- and pe.pricingDate<=''
        -- ORDER BY pe.pricingDate asc
    """
    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
                
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
            select s.securityid, c.companyid, c.simpleindustryid
            from ciqSecurity s 

            JOIN ciqCompany c ON c.companyid = s.companyid AND c.countryid = 213 --US, the company is based in the US
            AND c.companyId in ({', '.join([str(id) for id in ls_ids])})

            where s.primaryflag = 1
    """


    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
                
    afl = read_sql_to_df(sql_afl, connection, cursor)
    cid = read_sql_to_df(sql_cid, connection, cursor)

    return afl.merge(cid, left_on='securityid', right_on='securityid')

def get_afl_factor(date, ls_ids, factorids, connection = None):

    datestr = (
        pd.to_datetime(date)
            .tz_localize(SERVER_TIMEZONE)
            .tz_convert("UTC")
            .strftime("%Y-%m-%d")
    )

    sql = f"""
            select 
            dly.factorvalue
            -- , dly.iid,asofdate
            -- , dly.gvkey
            , dly.factorid
            -- , gvk.objectid
            -- , e.exchangeid
            -- , c.companyname
            , c.companyid
            -- , fx.currencyname
            -- , e.exchangename
            from ciqAFValuedailyna dly

            join ciqgvkeyiid gvk 
            on gvk.gvkey = dly.gvkey
            and gvk.iid = dly.iid
            and gvk.activeflag = 1 

            JOIN ciqTradingItem d ON gvk.objectId = d.tradingItemId
            JOIN ciqExchange e ON d.exchangeId = e.exchangeId
            JOIN ciqSecurity s ON s.securityid = d.securityid

            JOIN ciqCompany c ON c.companyid = s.companyid
            AND c.companyId in ({', '.join([str(id) for id in ls_ids])})


            JOIN ciqCurrency fx on fx.currencyid = d.currencyid 

            and d.currencyid = 160
            AND d.primaryflag = 1
            AND s.primaryflag = 1
            AND e.countryId = 213 --US, listed in US exchange
            AND c.countryid = 213 --US, the company is based in the US
            where dly.factorId in ({', '.join([str(factor_id) for factor_id in factorids])})
            and asOfDate= '{datestr}'
            
            order by dly.factorvalue desc
            """
    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
                
    return read_sql_to_df(sql, connection, cursor) 

def get_hist_earnings_dates(ls_ids, fromdate, todate, connection = None):
    """
        get the earning dates for a list of companies, the earning call dates are used as fallback
        notice, for example, 3102672 is of bad coverage

        note: I could not find a flag stating whether a event is for quarterly, semi-ann, annually
    
    """
    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()

    sql = f'''
            SELECT 

            kdo.keydevid
            ,co.companyid
            ,kd.headline
            ,kd.announceddate AS announceddateutc -- UTC
            ,kd.mostimportantdateutc -- UTC
            ,kd.entereddate AS entereddateET -- ET
            ,kde.keyDevEventTypeName
            ,kde.keyDevEventTypeId
            ,co.companyname

            FROM ciqkeydevtoobjecttoeventtype AS kdo 
            JOIN ciqkeydev AS kd ON kd.keydevid = kdo.keydevid
            JOIN ciqKeyDevEventType AS kde ON kde.keyDevEventTypeId = kdo.keyDevEventTypeid
            JOIN ciqcompany AS co ON co.companyid = kdo.objectid

            WHERE  
            1 = 1
            AND kd.mostimportantdateutc >= '{fromdate}' 
            AND '{todate}' >= kd.mostimportantdateutc
            AND kdo.keyDevEventTypeId in (28)   
            AND co.companyId in ({', '.join([str(id) for id in ls_ids])});

    ''' # have 28, 55 but only goes back to history
    return read_sql_to_df(sql, connection, cursor)  


def get_hist_earnings_release_dates(ls_ids, fromdate, todate, sortby = 'earningDates', connection = None):
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


def get_dps_pit(asofdate, connection = None):
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
    , EP.fiscalyear
    -- , EP.periodTypeId
    , EP.fiscalquarter
    -- , (select EPT.periodTypeName from ciqEstimatePeriodType EPT where EPT.periodTypeId = EP.periodTypeId) as periodTypeName
    , EP.periodenddate
    , EP.advancedate

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

    where EP.companyId = 24937
    -- and ED.dataItemId = 100208 -- mean
    and ED.dataItemId = 100209 -- median
    and '{asofdate}' between ED.effectiveDate and ED.toDate
    and EP.periodtypeid = 2
    ''' 
    return read_sql_to_df(sql, connection, cursor)  


# get stock split 
def get_stocksplit(ls_ids, asofdate, connection = None):

    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()

    sql = f"""
        select 

        s.securityid
        ,s.companyid
        ,split.exdate
        ,split.announceddate
        ,split.rate 
        
        from ciqSecurity s

        JOIN ciqTradingItem ti on ti.securityId=s.securityId
        JOIN ciqsplit split on split.tradingItemId=ti.tradingItemId
        
        WHERE s.companyId in ({', '.join([str(id) for id in ls_ids])})
        AND s.primaryflag=1
        AND ti.primaryflag=1        
        
        AND split.splittypeid = 12 

        and exdate = '{asofdate}';
            
        """
    
    df = read_sql_to_df(sql, connection, cursor)  
    df['rate'] = df['rate'].astype(float)
    return df 


def get_earnings_announcement_dates(ls_ids, fromdate, todate, connection = None):
    """
    """
    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()

    sql = f'''
            SELECT 

            ee.keydevid
            -- ,co.companyid
            ,e.headline
            ,e.announceddate -- UTC
            ,e.mostimportantdateutc -- UTC
            -- ,e.entereddate AS entereddateET -- ET
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
            -- AND et.keyDevEventTypeId in (12)

            -- limit 10 

    ''' # event table only has 55, 144 earnings release date 
        # do not have 28
        # goes back to history as well cover future

        # Mostly, 55 Earing Release Date event is at least one month earlier than the Earning Date
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
            -- AND et.keyDevEventTypeId IN (55, 144) 

    ''' # event table only has 55, 144 earnings release date 
        # do not have 28
        # goes back to history as well cover future

        # Mostly, 55 Earing Release Date event is at least one month earlier than the Earning Date
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
    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()

    sql = f"""
            SELECT keyDevId, transcriptid, transcriptCreationDateUTC
            
            FROM ciqTranscript
            
            WHERE 
            -- transcriptCollectionTypeId 7 --> spellchecked copy
            transcriptCollectionTypeId!=7
            AND
            transcriptCreationDateUTC >= '{startdatestr}'
            AND transcriptCreationDateUTC <= '{enddatestr}'

            ORDER BY transcriptCreationDateUTC asc;
            """    
    
    df = read_sql_to_df(sql, connection, cursor) 
    transcriptid = df.drop_duplicates(subset=['keydevid'], keep = 'first')['transcriptid'].unique()
    # print(transcriptid)
    
    sql = f"""
            SELECT t.transcriptId, t.transcriptCreationDateUTC, ete.objectid, t.keyDevId, t.transcriptCollectionTypeId,
            e.mostImportantDateUTC as EarningsDateUTC, e.announcedDateUTC, e.headline,
            eb.fiscalyear, eb.fiscalquarter, dr.delayReasonTypeId, drt.delayReasonTypeName, drt.isCancelledFlag, dr.delayReasonNotes
            FROM ciqTranscript t
            JOIN ciqEvent e ON e.keyDevId = t.keyDevId
            JOIN ciqEventToObjectToEventType ete ON ete.keyDevId = t.keyDevId
            JOIN ciqeventcallbasicinfo eb on eb.keyDevId = t.keyDevId
        	LEFT JOIN ciqTranscriptDelayReason dr on dr.keyDevId = t.keyDevId
        	LEFT JOIN ciqTranscriptDelayReasonType drt on dr.delayReasonTypeId = drt.delayReasonTypeId            
            WHERE ete.keyDevEventTypeId='48' --Earnings Calls
            AND t.transcriptId in ({', '.join([str(id) for id in transcriptid])}) 
            AND ete.objectid in ({', '.join([str(id) for id in ls_ids])})           
            ORDER BY t.transcriptCreationDateUTC asc;
            """    
                
    return read_sql_to_df(sql, connection, cursor) 

def get_transcript_ref_earliest_short(ls_ids, startdate, enddate=None, connection = None):
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
            -- SELECT keyDevId, transcriptid, transcriptCreationDateUTC
            SELECT *

            FROM ciqTranscript
            
            WHERE 
            -- transcriptCollectionTypeId 7 --> spellchecked copy
            transcriptCollectionTypeId!=7
            AND
            transcriptCreationDateUTC >= '{startdatestr}'
            AND transcriptCreationDateUTC <= '{enddatestr}'

            ORDER BY transcriptCreationDateUTC asc;
            """    
    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
    
    df = read_sql_to_df(sql, connection, cursor) 
    df = df.drop_duplicates(subset=['keydevid'], keep = 'first')
    return df  

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
    select tc.transcriptId, tc.componentOrder, tc.transcriptComponentTypeId, 
    tc.transcriptPersonId, CAST(tc.componentText AS TEXT) AS componentText, tct.transcriptComponentTypeName, tp.transcriptPersonName, tst.speakerTypeName
    from targetskma.ciqTranscriptComponent tc
    LEFT JOIN targetskma.ciqTranscriptPerson tp on tc.transcriptPersonId = tp.transcriptPersonId
    LEFT JOIN targetskma.ciqTranscriptSpeakerType tst on tst.speakerTypeId = tp.speakerTypeId
    LEFT JOIN targetskma.ciqProfessional pb on pb.proId= tp.proId
    LEFT JOIN targetskma.ciqTranscriptComponentType tct on tc.transcriptComponentTypeId = tct.transcriptComponentTypeId
    where transcriptId in ({', '.join([str(id) for id in ls_transcript_ids])}) 
    ORDER BY tc.componentOrder;
    """
    if connection is None:
        connection = get_connection(DBINFO)
    cursor = connection.cursor()
        
    return read_sql_to_df(sql, connection, cursor) 
# get afl live
if False:
    # asofdate = '2022-02-16'
    # logmarketcap = get_afl_factor(date = asofdate, factorid = 38).query('factorvalue >= 20') # e^20 = 485m

    # print(logmarketcap.head(50))

    # df = get_price_vol(date = asofdate, ls_ids = list(logmarketcap.companyid))
    # print(df)
    universe = pd.read_csv('~/Documents/production/universe/equityUS/2022/02/ALL.csv')['companyid'].unique()
    print(universe)

    vol = get_afl_factor_express(date = '2022-02-18', factorids = [47, ], ls_ids = universe).dropna() # e^20 = 485m
    print(vol)


if False:
    # df = get_target_price(asofdate = '2021-02-18')
    df = get_dps_pit(asofdate = '2022-03-16')
    df = df.query('fiscalyear >= 2022').sort_values(['fiscalyear', 'fiscalquarter'])
    print(df)




if False:

    universe = pd.read_csv('/home/zheng/datalake/universe/ALL_adjust/2022.csv')
    df = get_hist_earnings_release_dates(fromdate = '2022-06-01', todate = '2022-10-01', ls_ids = [258736],  
    connection = None)

    df.to_csv('tt.csv')
    print(df)

if True:
    universe = pd.read_csv('/home/zheng/datalake/universe/ALL_adjust/2022.csv')
    df = get_transcript_ref_earliest(ls_ids = universe['companyid'].unique(), startdate = '2021-12-03', enddate = '2022-12-30')
    print(df)

    # df = get_transcript([2228863, 2228859])
    # print(df.head(60))