from datetime import datetime


if STOCKS is None or len(STOCKS) == 0:
    check_basic_quality(logger, DB_HOST, TABLE_STOCK_INFO_NASDAQ, table_type='csv')
    check_basic_quality(logger, DB_HOST, TABLE_STOCK_INFO_NYSE, table_type='csv')
nasdaq_sdf = check_basic_quality(logger, DB_HOST, TABLE_SHORT_INTERESTS_NASDAQ)
nyse_sdf = check_basic_quality(logger, DB_HOST, TABLE_SHORT_INTERESTS_NYSE)

# Get the last date in short interests
logger.warn("YESTERDAY DATE: {}".format(YESTERDAY_DATE))

nasdaq_lastdate = nasdaq_sdf.agg({"Date": "max"}).first()[0]
logger.warn("NASDAQ short interest latest date: {}".format(nasdaq_lastdate))

nyse_maxdate = nyse_sdf.agg({"Date": "max"}).first()[0]
logger.warn("NYSE short interest latest date: {}".format(nyse_maxdate))
