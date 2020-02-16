sdf = spark.read.csv(DB_HOST+TABLE_SHORT_ANALYSIS, header=True)
logger.warn("sdf: {}".format(sdf))
rowcount = sdf.rdd.countApprox(timeout=10000, confidence=0.9)
logger.warn("Approx. number of rows: {}".format(rowcount))