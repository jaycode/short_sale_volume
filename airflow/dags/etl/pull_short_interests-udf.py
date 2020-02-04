# Use UDF to run requests GET on slave nodes.


def convert_data(olddata, symbol, url):
    col_names = olddata['dataset']['column_names']
    col_names.append('Symbol')
    col_names.append('SourceURL')
    col_names_multiplied = [col_names] * len(olddata['dataset']['data'])
    newdata = []
    for i, cols in enumerate(col_names_multiplied):
        datum = olddata['dataset']['data'][i]
        datum.append(symbol)
        datum.append(url)
        newdata.append(dict(zip(cols, datum)))
    return newdata


def pull_short_interests(exchange, host, info_table_path, short_interests_table_path):

    create_table = not(spark_table_exists(host, short_interests_table_path))
        
    def pull_exchange_short_interests_by_symbol(symbol):
        """
        Return:
            list of dicts [{'colname': value, ...}, ...]
        """
        if create_table == True:
            # If table does not exist, pull all data.
            url = 'https://www.quandl.com/api/v3/datasets/FINRA/'+exchange+'_{}?start_date='+START_DATE+'&end_date='+YESTERDAY_DATE+'&api_key='+QUANDL_API_KEY
        else:
            # If table had existed, pull yesterday's data.
            url = 'https://www.quandl.com/api/v3/datasets/FINRA/'+exchange+'_{}?start_date='+YESTERDAY_DATE+'&end_date='+YESTERDAY_DATE+'&api_key='+QUANDL_API_KEY

        url = url.format(symbol)
        response = requests.get(url)
        newdata = []
        if response.status_code in [200, 201]:
            newdata = convert_data(response.json(), symbol, url)
        return newdata

    
    # [{'colname': value, ...}, ...]
    schema = T.ArrayType(
                T.MapType(
                    T.StringType(), T.StringType()
                )
             )
    udf_pull_exchange_short_interests = F.udf(pull_exchange_short_interests_by_symbol, schema)

    # Prepare list of stocks
    if STOCKS is not None and len(STOCKS) > 0:
        rdd1 = spark.sparkContext.parallelize(STOCKS)
        row_rdd = rdd1.map(lambda x: Row(x))
        df = spark.createDataFrame(row_rdd,['Symbol'])
    else:
        df = spark.read.parquet(host+info_table_path)
        if LIMIT is not None:
            df = df.limit(LIMIT)

    df = df.withColumn('short_interests', udf_pull_exchange_short_interests('Symbol'))

    # Convert [short_interests: [{col: val, ...}, ...]] to
    # [{col: val, ...}, ...]
    df = df.select(F.explode(df['short_interests']).alias('col')) \
         .rdd.map(lambda x: x['col'])

    df_schema = T.StructType([T.StructField('Date', T.StringType(), False),
                              T.StructField('ShortExemptVolume', T.StringType(), True),
                              T.StructField('ShortVolume', T.StringType(), True),
                              T.StructField('Symbol', T.StringType(), False),
                              T.StructField('TotalVolume', T.StringType(), True),
                              T.StructField('SourceURL', T.StringType(), True),
                             ])
    df = spark.createDataFrame(df, df_schema)
    df = df.withColumn('Date', df['Date'].cast(T.DateType())) \
         .withColumn('ShortExemptVolume', df['ShortExemptVolume'].cast(T.DoubleType())) \
         .withColumn('ShortVolume', df['ShortVolume'].cast(T.DoubleType())) \
         .withColumn('TotalVolume', df['TotalVolume'].cast(T.DoubleType()))

    if create_table:
        logger.warn("Creating table {}".format(host+short_interests_table_path))
        df.write.mode('overwrite').format('csv').save(host+short_interests_table_path, header=True)
    else:
        logger.warn("Appending to table {}".format(host+short_interests_table_path))
        df.write.mode('append').format('csv').save(host+short_interests_table_path, header=True)
        
        # Drop duplicates later when we combine the datasets:
        # 1. We do not want to waste S3 bandwidth.
        # 2. Raw data are meant to be dirty. We are going to use only the final dataset for analysis.
        # 3. If we really want to clean the datasets. Create another DAG for that.
        # code:
#         spark.read.parquet(host+short_interests_table_path).dropDuplicates(['Date']) \
#         .write.mode('append').parquet(host+short_interests_table_path)
        
    logger.warn("done!")

pull_short_interests('FNSQ', DB_HOST, TABLE_STOCK_INFO_NASDAQ, TABLE_SHORT_INTERESTS_NASDAQ)
pull_short_interests('FNYX', DB_HOST, TABLE_STOCK_INFO_NYSE, TABLE_SHORT_INTERESTS_NYSE)