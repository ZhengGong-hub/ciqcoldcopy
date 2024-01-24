def query_1_sql(ls_ids, fys):
    '''
    ls_ids: companyid list
    fys:    fiscalyear list
    '''

    return f"""
    SELECT et.objectId AS CompanyID,
           e.keyDevId, e.mostImportantDateUTC AS EarningsDate,
           em.marketIndicatorTypeName,
           e.headline,
           er.fiscalyear,
           er.fiscalquarter,
           er.calendaryear
    FROM ciqevent e
    JOIN ciqEventToObjectToEventType et ON et.keyDevId = e.keyDevId
    JOIN ciqEventERInfo              er ON e.keyDevId = er.keyDevId
    JOIN ciqEventMarketIndicatorType em ON em.marketIndicatorTypeId = er.marketIndicatorTypeId
    WHERE et.objectId IN ({', '.join([str(id) for id in ls_ids])}) AND
          er.fiscalyear IN ({', '.join([str(fy) for fy in fys])})
    """
