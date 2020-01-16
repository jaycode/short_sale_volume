create_table = not(spark_table_exists(DB_HOST, TABLE_SHORT_ANALYSIS))

sdf_shorts = spark.read.parquet(DB_HOST+TABLE_SHORT_INTERESTS_NASDAQ, DB_HOST+TABLE_SHORT_INTERESTS_NYSE)
sdf_shorts = sdf_shorts.groupby(['Date', 'Symbol']) \
                 .agg(F.sum(sdf_shorts['ShortExemptVolume']).alias('short_exempt_volume'),
                      F.sum(sdf_shorts['ShortVolume']).alias('short_volume'),
                      F.sum(sdf_shorts['TotalVolume']).alias('total_volume'),
                      F.first(sdf_shorts['SourceURL']).alias('source_url')
                     )
sdf_prices = spark.read.csv(DB_HOST+TABLE_STOCK_PRICES, header=True, inferSchema=True) \
             .dropDuplicates(['date', 'symbol'])
sdf_prices = sdf_prices.withColumn('date', sdf_prices['date'].cast(T.DateType()))

if create_table == False:
    sdf_shorts = sdf_shorts.filter(sdf_shorts['Date'] >= F.to_date(F.lit(YESTERDAY_DATE)))
    sdf_prices = sdf_prices.filter(sdf_prices['date'] >= F.to_date(F.lit(YESTERDAY_DATE)))

sdf_short_analysis = sdf_shorts.join(sdf_prices, (sdf_shorts['Date'] == sdf_prices['date']) & \
                                     (sdf_shorts['Symbol'] == sdf_prices['symbol']), how='inner') \
                               .drop(sdf_shorts['Date']).drop(sdf_shorts['Symbol'])

mode = 'overwrite'
if create_table == False:
    logger.warn("Appending to table {}".format(DB_HOST+TABLE_SHORT_ANALYSIS))
    mode = 'append'
else:
    logger.warn("Creating table {}".format(DB_HOST+TABLE_SHORT_ANALYSIS))

sdf_short_analysis.write.mode(mode).parquet(DB_HOST+TABLE_SHORT_ANALYSIS)
logger.warn("done!")