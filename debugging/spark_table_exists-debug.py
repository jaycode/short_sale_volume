table_exists = spark_table_exists('s3a://short-interest-effect', '/data/raw/short_interests_nasdaq')
print("Table exists? {}".format(table_exists))