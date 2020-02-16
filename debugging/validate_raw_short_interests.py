sdf = spark.read.csv(DB_HOST+TABLE_SHORT_INTERESTS_NASDAQ, header=True)
logger.warn("sdf: {}".format(sdf))

# symbol = 'GOOG'
# last_date = sdf.where(sdf.Symbol == F.lit(symbol)) \
#                 .agg({"Date": "max"}).first()[0]
# logger.warn("last date for {} is {}".format(symbol, last_date))

last_dates = sdf.groupBy('Symbol').agg(F.max('Date').alias('last_date')).collect()
logger.warn("last_dates length: {}".format(len(last_dates)))
logger.warn("last_dates first: {}".format(last_dates[0]))

