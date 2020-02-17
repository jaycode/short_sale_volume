from py4j.protocol import Py4JJavaError

def pull_stock_info(url, db_host, table_path):
    response = requests.get(url)
    if response.status_code == 200 or response.status_code == 201:
        content = response.content.decode('utf-8')
        content = content.replace('Summary Quote', 'SummaryQuote')
        delete_path(spark, db_host, table_path)
        df = spark.createDataFrame([[content]], ['info_csv'])

        df.rdd.map(lambda x: x['info_csv'].replace("[","").replace("]", "")).saveAsTextFile(db_host+table_path+'-temp')
        logger.warn("Stored data from {} to {}.".format(url, db_host+table_path+'-temp'))

        # Some tickers, like "BRK.A", should be changed to "BRK_S" instead.
        df = spark.read.csv(db_host+table_path+'-temp', header=True)
        # replace_dots = F.udf(lambda x: x.replace('.', '_'), T.StringType())
        # df = df.withColumn('Symbol', F.when(F.col('Symbol').contains('.'), replace_dots('Symbol')).otherwise(F.col('Symbol')))
        df = df.withColumn('Symbol', F.regexp_replace('Symbol', '\.', '_')) \
             .write.mode('overwrite').format('csv').save(db_host+table_path, header=True)
             
        delete_path(spark, db_host, table_path+'-temp')

    else:
        logger.warn("Failed to connect to {}. We will use existing stock info data if they have been created.".format(url))
        
    
pull_stock_info(URL_NASDAQ, DB_HOST, TABLE_STOCK_INFO_NASDAQ)
pull_stock_info(URL_NYSE, DB_HOST, TABLE_STOCK_INFO_NYSE)