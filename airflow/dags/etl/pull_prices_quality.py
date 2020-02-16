if STOCKS is None or len(STOCKS) == 0:
    check_basic_quality(logger, DB_HOST, TABLE_STOCK_INFO_NASDAQ, table_type='csv')
    check_basic_quality(logger, DB_HOST, TABLE_STOCK_INFO_NYSE, table_type='csv')
sdf = check_basic_quality(logger, DB_HOST, TABLE_STOCK_PRICES, table_type='csv')

# Get the last date in prices
logger.warn("YESTERDAY DATE: {}".format(YESTERDAY_DATE))
lastdate = sdf.agg({"date": "max"}).first()[0]
logger.warn("Prices latest date: {}".format(lastdate))

last100dates = sdf.select('date').where(sdf.date != 'date').orderBy(F.desc('date')).take(100)
logger.warn("Last 100 dates: {}".format(last100dates))