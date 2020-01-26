START_DAY = START_DATE.split('-')[2]
# In QuoteMedia, months start from 0, so we adjust this variable.
START_MONTH = int(START_DATE.split('-')[1]) - 1
START_YEAR = START_DATE.split('-')[0]

YST_DAY = YESTERDAY_DATE.split('-')[2]
# In QuoteMedia, months start from 0, so we adjust this variable.
YST_MONTH = int(YESTERDAY_DATE.split('-')[1]) - 1
YST_YEAR = YESTERDAY_DATE.split('-')[0]

create_table = not(spark_table_exists(DB_HOST, TABLE_STOCK_PRICES))
def pull_prices_by_symbol(symbol):
    """
    Return:
        list of dicts [{'colname': value, ...}, ...]
    """
    if create_table == True:
        # If table does not exist, pull all data.
        url = URL.format(sd=START_DAY, sm=START_MONTH, sy=START_YEAR,
                         ed=YST_DAY, em=YST_MONTH, ey=YST_YEAR,
                         sym=symbol)
    else:
        # If table had existed, pull yesterday's data.
        url = URL.format(sd=YST_DAY, sm=YST_MONTH, sy=YST_YEAR,
                         ed=YST_DAY, em=YST_MONTH, ey=YST_YEAR,
                         sym=symbol)
        
    # Code for always overwrite without temp table
#     url = URL.format(sd=START_DAY, sm=START_MONTH, sy=START_YEAR,
#                      ed=YST_DAY, em=YST_MONTH, ey=YST_YEAR,
#                      sym=symbol)

    response = requests.get(url)
    newdata = ""
    if response.status_code in [200, 201]:
        newdata = response.content.decode('utf-8')
        newdata = newdata.replace('\n', ','+symbol+'\n')
        newdata = newdata.replace('tradevol,'+symbol+'\n', 'tradevol,symbol\n')
    return newdata

schema = T.StringType()
udf_pull_prices = F.udf(pull_prices_by_symbol, schema)
    
# Prepare list of stocks
if STOCKS is not None and len(STOCKS) > 0:
    rdd1 = spark.sparkContext.parallelize(STOCKS)
    row_rdd = rdd1.map(lambda x: Row(x))
    df = spark.createDataFrame(row_rdd,['Symbol'])
else:
    df = spark.read.csv([DB_HOST+TABLE_STOCK_INFO_NASDAQ,
                        DB_HOST+TABLE_STOCK_INFO_NYSE], header=True) \
         .select('Symbol').dropDuplicates()
    if LIMIT is not None:
        df = df.limit(LIMIT)

df = df.withColumn('prices_csv', udf_pull_prices('Symbol'))

df = df.select('prices_csv').where(df['prices_csv'] != '')

table_name = DB_HOST+TABLE_STOCK_PRICES
mode = 'overwrite'
if create_table:
    logger.warn("Creating table {}".format(table_name))    
else:
    logger.warn("Appending to table {}".format(table_name))
    mode = 'append'

# Repartition here is important so we may end up with multiple CSV-like files.
# Without repartition, the headers are going to be written multiple times
# in a single csv file.
tempdir = DB_HOST+TABLE_STOCK_PRICES+'-temp'
logger.warn("    Creating temporary table {}".format(tempdir))

numrows = df.count()
df \
    .repartition(numrows).write.mode('overwrite') \
    .csv(tempdir, header=False, quote=" ")


if create_table:
    logger.warn("    done! Now creating table {}".format(table_name))
else:
    logger.warn("    done! Now appending to table {}".format(table_name))

spark.read.csv(tempdir, header=True, ignoreLeadingWhiteSpace=True, inferSchema=True) \
.write.mode(mode).csv(table_name, header=True)
# .write.mode(mode).parquet(DB_HOST+TABLE_STOCK_PRICES)


logger.warn("done!")