# DataFrame[short_exempt_volume: string, short_volume: string, total_volume: string, date: string, open: string, 
# high: string, low: string, close: string, volume: string, changed: string, changep: string, adjclose: string, 
# tradeval: string, tradevol: string, symbol: string]

sdf = spark.read.csv(DB_HOST+TABLE_SHORT_ANALYSIS, header=True)

# Correct all Quantopian errors here.
# -----------
sdf.withColumn('symbol', F.when(F.col('symbol')=='GECCL', 'GECC_L').otherwise(F.col('symbol')))
# -----------

sdf.select(['date', 'symbol', 'short_exempt_volume', 'short_volume', 'total_volume']) \
   .coalesce(1).write.mode('overwrite').csv(DB_HOST+TABLE_SHORT_ANALYSIS_QUANTOPIAN, header=True)

delete_path(spark, DB_HOST, TABLE_SHORT_ANALYSIS_QUANTOPIAN+".csv")
copyMerge(spark, DB_HOST, DB_HOST+TABLE_SHORT_ANALYSIS_QUANTOPIAN, DB_HOST+TABLE_SHORT_ANALYSIS_QUANTOPIAN+".csv")
delete_path(spark, DB_HOST, TABLE_SHORT_ANALYSIS_QUANTOPIAN)

logger.warn("done!")