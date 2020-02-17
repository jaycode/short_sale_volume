from datetime import datetime

nasdaq_sdf = check_basic_quality(logger, DB_HOST, TABLE_SHORT_INTERESTS_NASDAQ)
nyse_sdf = check_basic_quality(logger, DB_HOST, TABLE_SHORT_INTERESTS_NYSE)

# Get the last date in short interests
logger.warn("YESTERDAY DATE: {}".format(YESTERDAY_DATE))

def check_data_quality(sdf, exchange):
    # row = sdf.agg({"Date": "max"}).first()
    row = sdf.orderBy([F.col('Date').desc()]).first()
    logger.warn("row: {}".format(row))
    lastdate = row['Date']
    url = row['SourceURL']
    response_json = requests.get(url).json()
    newest_available_date = response_json['dataset']['newest_available_date']
    symbol = row['Symbol']

    logger.warn("{}/{} short interest latest date: {}. Quandl newest_available_date: {}".format(exchange, symbol, lastdate, newest_available_date))
    if lastdate == newest_available_date:
        logger.warn("(SUCCESS) Latest date in database equals Quandl's latest available date.")
    else:
        logger.warn("(FAIL) Latest date in database does not equal Quandl's latest available date. Data probably not saved after pulling short interests.")

check_data_quality(nasdaq_sdf, 'NASDAQ')
check_data_quality(nyse_sdf, 'NYSE')
