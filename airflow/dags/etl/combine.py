create_table = not(spark_table_exists(DB_HOST, TABLE_SHORT_ANALYSIS))

sdf_shorts = spark.read.csv([DB_HOST+TABLE_SHORT_INTERESTS_NASDAQ, DB_HOST+TABLE_SHORT_INTERESTS_NYSE], header=True, inferSchema=True)
sdf_shorts = sdf_shorts.withColumn('Date', sdf_shorts['Date'].cast(T.DateType()))

sdf_shorts = sdf_shorts.groupby(['Date', 'Symbol']) \
                 .agg(F.sum(sdf_shorts['ShortExemptVolume']).alias('short_exempt_volume'),
                      F.sum(sdf_shorts['ShortVolume']).alias('short_volume'),
                      F.sum(sdf_shorts['TotalVolume']).alias('total_volume')
                     )

if create_table == False:
    sdf_shorts = sdf_shorts.filter(sdf_shorts['Date'] >= F.to_date(F.lit(YESTERDAY_DATE)))

sdf_short_analysis = sdf_shorts

mode = 'overwrite'
if create_table == False:
    logger.warn("Appending to table {}".format(DB_HOST+TABLE_SHORT_ANALYSIS))
    mode = 'append'
else:
    logger.warn("Creating table {}".format(DB_HOST+TABLE_SHORT_ANALYSIS))

# Coalesce is a must to avoid multiple headers.
sdf_short_analysis.coalesce(1).write.mode(mode).csv(DB_HOST+TABLE_SHORT_ANALYSIS, header=True)

delete_path(spark, DB_HOST, TABLE_SHORT_ANALYSIS+".csv")
copyMerge(spark, DB_HOST, DB_HOST+TABLE_SHORT_ANALYSIS, DB_HOST+TABLE_SHORT_ANALYSIS+".csv")

logger.warn("done!")
