import pandas as pd 
import psycopg2
import json

def read_sql_to_df(sql):
    """Internal: execute sql and get dataframe as a return 
    
    Args:
        sql (str): sql to be executed 
        db (psycopg2.connect): connect to ciq target database 
        cursor (database connection.cursor): 
    
    Returns:
        pd.DataFrame: contains result for executed sql 
    """
    connect_info = json.load(open('cfg/aws.json', 'r'))
    connection = get_connection(connect_info)
    cursor = connection.cursor()

    try:
        cursor.execute(sql)
        data = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        return pd.DataFrame(data,columns = columns)
    except (Exception, psycopg2.DatabaseError) as error:
        connection.rollback()
        print("Error: %s" % error)


def get_connection(dbInfo):
    """get the db connection using info in dbInfo json file
    
    Args:
        dbInfo (.json): user-pw for ciq target database 
    
    Returns:
        psycopg2.connect
    """
    # config parameters of db connection

    db = psycopg2.connect(
        host=dbInfo['host'],
        database=dbInfo["dbname"],
        user=dbInfo["user"],
        password=dbInfo["pwd"],
        port=dbInfo['port'])
    

    print('DB connection set up!\n')
    return db


def estimatesQ(ls_ids, dataitemids, datestart):
    sql= f"""
    CREATE TEMP TABLE PC AS
         SELECT 
            -- EP.estimatepriodid,
            -- EP.fiscalchainseriesid,
            -- EP.periodtypeid,
            EP.companyid,
            EP.fiscalquarter,
            EP.fiscalyear,
            EP.calendarquarter,
            EP.calendaryear,
            EP.periodenddate,
            EP.advancedate,
                EC.tradingitemid,
                EC.estimateConsensusId
         FROM ciqEstimatePeriod EP
                JOIN ciqEstimateConsensus   EC ON EC.estimatePeriodId = EP.estimatePeriodId
         WHERE EP.companyId IN ({', '.join([str(id) for id in ls_ids])})
               AND EP.periodTypeId = 2 -- Quarter 
               AND EP.periodenddate > '{datestart}';
    CREATE INDEX idx_pc ON PC (estimateConsensusId);

    SELECT
           PC.*,
           D.dataitemname,
           ED.dataitemid,
           ED.currencyId,
           FX.isocode,
           ED.dataItemValue,
           ED.effectiveDate,
           ED.toDate,
           ED.estimatescaleid,
           SCALE.estimatescalename
    FROM ciqEstimateNumericData ED
       left JOIN ciqdataitem as D on ED.dataitemid = D.dataitemId -- explanational table
       left JOIN ciqcurrency as FX on FX.currencyid = ED.currencyId -- explanational table
       left JOIN ciqestimatescaletype as SCALE on SCALE.estimatescaleid = ED.estimatescaleid -- explanational table
        JOIN PC ON ED.estimateConsensusId = PC.estimateConsensusId
    WHERE ED.dataItemId IN ({', '.join([str(id) for id in dataitemids])});
    """
    return read_sql_to_df(sql)

def get_transcript(ls_ids, startdatestr, enddatestr):
    sql = f"""
    CREATE TEMP TABLE t_transcriptId AS
        SELECT DISTINCT a.transcriptId FROM targetskma.ciqTranscript a, (
            SELECT keyDevId, Min(transcriptCreationDateUTC) AS minTm FROM targetskma.ciqTranscript
                WHERE transcriptCollectionTypeId!=7 GROUP BY keyDevId) mtm
        WHERE a.keyDevId = mtm.keyDevId AND a.transcriptCreationDateUTC = mtm.minTm
        ORDER BY transcriptId;
    CREATE INDEX t_idx ON t_transcriptId (transcriptId);

    CREATE TEMP TABLE t_ete AS
        SELECT  t.transcriptId,
                t.transcriptCreationDateUTC,
                t.transcriptCollectionTypeId,
                t.keyDevId,
                ete.objectId AS companyId
        FROM targetskma.ciqTranscript                   t
        JOIN t_transcriptId                         tid ON tid.transcriptId = t.transcriptId
        JOIN targetskma.ciqEventToObjectToEventType ete ON ete.keyDevId = t.keyDevId
        WHERE  ete.keyDevEventTypeId='48' --Earnings Calls
            AND ete.objectId IN ({', '.join([str(id) for id in ls_ids])})
            -- AND comp.companyid IN (SELECT companyid FROM {{tschema}}.temp_universe)
            AND t.transcriptCreationDateUTC BETWEEN '{startdatestr}' AND '{enddatestr}'
            -- AND t.transcriptCreationDateUTC >= '{startdatestr}'
            -- AND t.transcriptCreationDateUTC <= '{enddatestr}'           
        ORDER BY t.transcriptCreationDateUTC asc;
    DROP TABLE t_transcriptId;
    CREATE INDEX t_idx ON t_ete (keyDevId);

    CREATE TEMP TABLE t_ete_ebdr AS
        SELECT  t.transcriptId,
                t.transcriptCreationDateUTC,
                t.transcriptCollectionTypeId,
                t.keyDevId,
                t.companyId,
                e.mostImportantDateUTC as EarningsDateUTC,
                e.announcedDateUTC,
                eb.fiscalyear,
                eb.fiscalquarter,
                dr.delayReasonTypeId
        FROM t_ete                                    t
        JOIN targetskma.ciqEvent                      e  ON  e.keyDevId = t.keyDevId
        JOIN targetskma.ciqeventcallbasicinfo         eb ON eb.keyDevId = t.keyDevId
        LEFT JOIN targetskma.ciqTranscriptDelayReason dr ON dr.keyDevId = t.keyDevId;
    DROP TABLE t_ete;
    CREATE INDEX t_idx ON t_ete_ebdr (transcriptId);

    CREATE TEMP TABLE t_ete_ebdr_tc AS
        SELECT  t.transcriptId,
                t.transcriptCreationDateUTC,
                t.transcriptCollectionTypeId,
                t.keyDevId,
                t.companyId,
                t.EarningsDateUTC,
                t.announcedDateUTC,
                t.fiscalyear,
                t.fiscalquarter,
                t.delayReasonTypeId,
                CAST(tc.componentText AS TEXT) AS componenttext,
                tc.transcriptComponentId,
                tc.componentOrder,
                tc.transcriptComponentTypeId,
      	        tc.transcriptpersonid    --no need select
        FROM t_ete_ebdr                        t
        JOIN targetskma.ciqTranscriptComponent tc ON tc.transcriptId = t.transcriptId;
    DROP TABLE t_ete_ebdr;
    CREATE INDEX t_idx ON t_ete_ebdr_tc (transcriptComponentTypeId);
    CREATE INDEX t_idx1 ON t_ete_ebdr_tc (transcriptpersonid);

    CREATE TEMP TABLE t_ete_ebdr_tctyp AS
        SELECT  t.transcriptId,
                t.transcriptCreationDateUTC,
                t.transcriptCollectionTypeId,
                t.keyDevId,
                t.companyId,
                t.EarningsDateUTC,
                t.announcedDateUTC,
                t.fiscalyear,
                t.fiscalquarter,
                t.delayReasonTypeId,
                t.componenttext,
                t.transcriptComponentId,
                t.componentOrder,
                t.transcriptComponentTypeId,
      	        t.transcriptpersonid,    --no need select
                tcty.transcriptComponentTypeName,
                p.speakerTypeId,         -- no need select
                p.proId                  -- no need select
        FROM t_ete_ebdr_tc                         t
        JOIN targetskma.ciqTranscriptComponentType tcty ON tcty.transcriptComponentTypeId = t.transcriptComponentTypeId
        JOIN targetskma.ciqTranscriptPerson        p    ON p.transcriptpersonid = t.transcriptpersonid;
    DROP TABLE t_ete_ebdr_tc;

    SELECT  t.transcriptId,
            t.transcriptCreationDateUTC,
            t.transcriptCollectionTypeId,
            t.keyDevId,
            t.companyId,
            t.EarningsDateUTC,
            t.announcedDateUTC,
            t.fiscalyear,
            t.fiscalquarter,
            t.delayReasonTypeId,
            t.componenttext,
            t.transcriptComponentId,
            t.componentOrder,
            t.transcriptComponentTypeId,
            t.transcriptComponentTypeName,
	        pt.speakerTypeName,
            pb.title,
            pb.personid,
            pb.companyid as pOrg
    FROM t_ete_ebdr_tctyp                    t
    JOIN targetskma.ciqTranscriptSpeakerType pt ON pt.speakerTypeId = t.speakerTypeId
    JOIN targetskma.ciqProfessional          pb ON pb.proId= t.proId;
    """
    return read_sql_to_df(sql)