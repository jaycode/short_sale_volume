# ----------------
# Combine short interest tables
# ----------------

# Without a schema (even with inferSchema=True), the SUM aggregation fails.
schema = T.StructType([
    T.StructField("Date", T.StringType(), True),
    T.StructField("ShortExemptVolume", T.FloatType(), True),
    T.StructField("ShortVolume", T.FloatType(), True),
    T.StructField("SourceURL", T.StringType(), True),
    T.StructField("Symbol", T.StringType(), True),
    T.StructField("TotalVolume", T.FloatType(), True),
])

sdf_shorts = spark.read.format('csv') \
                  .option('header', True) \
                  .option('schema', schema) \
                  .option('mode', 'DROPMALFORMED') \
                  .load([DB_HOST+TABLE_SHORT_INTERESTS_NASDAQ, DB_HOST+TABLE_SHORT_INTERESTS_NYSE])

rows = sdf_shorts.where((F.col('Symbol') == 'SPY') & (F.col('Date') == '2020-02-21')).collect()
logger.warn("Rows: {}".format(rows))

sdf_shorts = sdf_shorts.groupBy('Date', 'Symbol') \
                 .agg(F.sum('ShortExemptVolume').alias('short_exempt_volume'),
                      F.sum('ShortVolume').alias('short_volume'),
                      F.sum('TotalVolume').alias('total_volume')
                     ) \
                 .withColumnRenamed('Date', 'date') \
                 .withColumnRenamed('Symbol', 'symbol')

# ----------------
# Prepare for Quantopian
# ----------------

# DataFrame[short_exempt_volume: string, short_volume: string, total_volume: string, date: string, open: string, 
# high: string, low: string, close: string, volume: string, changed: string, changep: string, adjclose: string, 
# tradeval: string, tradevol: string, symbol: string]

# Correct all Quantopian errors here.
# -----------
sdf = sdf_shorts.withColumn('symbol', F.when(F.col('symbol')=='GECCL', 'GECC_L').otherwise(F.col('symbol')))
# -----------

sdf.select(['date', 'symbol', 'short_exempt_volume', 'short_volume', 'total_volume']) \
   .coalesce(1).write.mode('overwrite').csv(DB_HOST+TABLE_SHORT_ANALYSIS, header=True)

delete_path(spark, DB_HOST, TABLE_SHORT_ANALYSIS+".csv")
copyMerge(spark, DB_HOST, DB_HOST+TABLE_SHORT_ANALYSIS, DB_HOST+TABLE_SHORT_ANALYSIS+".csv")
delete_path(spark, DB_HOST, TABLE_SHORT_ANALYSIS)

logger.warn("done!")
