# 1. Latest available date in the database should be the same with newest_available_date in Quandl.
# 2. Short volume for that date should be the same too.

from datetime import datetime
import numpy as np

nasdaq_sdf = check_basic_quality(logger, DB_HOST, TABLE_SHORT_INTERESTS_NASDAQ)
nyse_sdf = check_basic_quality(logger, DB_HOST, TABLE_SHORT_INTERESTS_NYSE)

# Get the last date in short interests
logger.warn("PULL DATE: {}".format(PULL_DATE))

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
        logger.warn("(FAIL) Latest date in database does not equal Quandl's latest available date. Make sure data are saved and the PULL_DATE is correct.")

    short_volume_data = float(row['ShortVolume'])
    short_volume_url = float(response_json['dataset']['data'][0][1])
    logger.warn("short_volume_data: {}, short_volume_url: {}".format(short_volume_data, short_volume_url))
    if np.isclose(short_volume_data, short_volume_url):
        logger.warn("(SUCCESS) Same short interest volume ({})".format(short_volume_data))
    else:
        logger.warn("(FAIL) short interest volume in the database: {}, in the url: {}".format(short_volume_data, short_volume_url))


check_data_quality(nasdaq_sdf, 'NASDAQ')
check_data_quality(nyse_sdf, 'NYSE')
