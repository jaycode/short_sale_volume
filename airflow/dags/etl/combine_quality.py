# Use SPY for debugging
# 1. Total volume of Short Interests in TABLE_SHORT_ANALYSIS and in TABLE_SHORT_INTERESTS_NASDAQ + TABLE_SHORT_INTERESTS_NYSE should be the same.
# 2. Total volume of Short Interests in TABLE_SHORT_INTERESTS_NASDAQ should be the same from the one on Quandl website.
# 3. Total volume of Short Interests in TABLE_SHORT_INTERESTS_NYSE should be the same from the one on Quandl website.

import numpy as np

anly_sdf = spark.read.csv(DB_HOST+TABLE_SHORT_ANALYSIS+".csv", header=True)

row = anly_sdf.where(F.col("symbol") == 'SPY') \
              .sort(F.col('date').desc()).first()
logger.warn("row: {}".format(row))
logger.warn("Last date of SPY: {}".format(row['date']))
last_date = row['date']
if last_date == None:
    logger.warn("(FAIL) Last date cannot be found. Data were not imported properly.")
else:
    short_volume = float(row['short_volume'])
    logger.warn("Short Volume of SPY in {} for date {}: {}".format(DB_HOST+TABLE_SHORT_ANALYSIS+".csv", last_date, short_volume))

    sdf1 = spark.read.csv(DB_HOST+TABLE_SHORT_INTERESTS_NASDAQ, header=True)
    sdf1 = sdf1.where((F.col("Symbol") == 'SPY') & (F.col("Date") == last_date))
    row1 = sdf1.first()
    # logger.warn("Row1: {}".format(row1))
    sv1 = float(row1['ShortVolume'])
    logger.warn("Short Volume of SPY from NASDAQ exchange for date {}: {}".format(last_date, sv1))

    sdf2 = spark.read.csv(DB_HOST+TABLE_SHORT_INTERESTS_NYSE, header=True)
    sdf2 = sdf2.where((F.col("Symbol") == 'SPY') & (F.col("Date") == last_date))
    row2 = sdf2.first()
    # logger.warn("Row2: {}".format(row2))
    sv2 = float(row2['ShortVolume'])
    logger.warn("Short Volume of SPY from NYSE exchange for date {}: {}".format(last_date, sv2))
    source2 = row2['ShortVolume']

    if np.isclose(short_volume, sv1 + sv2):
        logger.warn("(SUCCESS) The total short volume in {} is equal to the sum of short volumes from the exchanges.".format(DB_HOST+TABLE_SHORT_ANALYSIS+".csv"))
    else:
        logger.warn("(FAIL) The total short volume in {} is not equal to the sum of short volumes from the exchanges. (value is {}, should be {})".format(DB_HOST+TABLE_SHORT_ANALYSIS+".csv", short_volume, sv1 + sv2))
