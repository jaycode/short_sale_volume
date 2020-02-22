table_exists = spark_table_exists(DB_HOST, TABLE_SHORT_INTERESTS_NYSE)
logger.warn("Table TABLE_SHORT_INTERESTS_NYSE exists? {}".format(table_exists))

table_exists = spark_table_exists(DB_HOST, TABLE_SHORT_INTERESTS_NASDAQ)
logger.warn("Table TABLE_SHORT_INTERESTS_NASDAQ exists? {}".format(table_exists))