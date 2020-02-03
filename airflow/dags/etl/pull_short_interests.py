from datetime import datetime

def a_before_b(a, b):
    date_format = "%Y-%m-%d"

    # create datetime objects from the strings
    da = datetime.strptime(a, date_format)
    db = datetime.strptime(b, date_format)

    if da < db:
        return True
    else:
        return False
    
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


def pull_short_interests(exchange, host, info_table_path, short_interests_table_path, log_every_n=100):
        
    def pull_exchange_short_interests_by_symbol(symbol, start_date, end_date):
        """
        Return:
            list of dicts [{'colname': value, ...}, ...]
        """
        url = 'https://www.quandl.com/api/v3/datasets/FINRA/'+exchange+'_{}?start_date='+start_date+'&end_date='+end_date+'&api_key='+QUANDL_API_KEY
        url = url.format(symbol)
        response = requests.get(url)
        newdata = []
        if response.status_code in [200, 201]:
            newdata = convert_data(response.json(), symbol, url)
        return newdata

    # Prepare list of stocks
    if STOCKS is not None and len(STOCKS) > 0:
        rdd1 = spark.sparkContext.parallelize(STOCKS)
        row_rdd = rdd1.map(lambda x: Row(x))
        df = spark.createDataFrame(row_rdd,['Symbol'])
    else:
        df = spark.read.csv(host+info_table_path, header=True)
        if LIMIT is not None:
            df = df.limit(LIMIT)
    symbols = df.select('Symbol').rdd.map(lambda r: r['Symbol']).collect()
    
    table_exists = spark_table_exists(host, short_interests_table_path)
    if table_exists:
        short_sdf = spark.read.csv(host+short_interests_table_path, header=True)
        
    total_rows = 0
    for i, symbol in enumerate(symbols):
        if table_exists:
            # Get the last date of a stock. If this last date >= YESTERDAY_DATE, don't do anything.
            dates = short_sdf.select('Date').where(short_sdf.Symbol == F.lit(symbol)) \
                .orderBy(F.desc('Date')).take(1)
            if len(dates) > 0:
                if a_before_b(dates[0].Date, YESTERDAY_DATE):
                    data = pull_exchange_short_interests_by_symbol(symbol, dates[0].Date, YESTERDAY_DATE)
                else:
                    data = []
            else:
                data = pull_exchange_short_interests_by_symbol(symbol, START_DATE, YESTERDAY_DATE)
        else:
            data = pull_exchange_short_interests_by_symbol(symbol, START_DATE, YESTERDAY_DATE)
        total_rows += len(data)
        if (i%log_every_n == 0 or (i+1) == len(symbols)):
            logger.warn("downloading from exchange {} - {}/{} - total rows in this batch: {}".format(exchange, i+1, len(symbols), total_rows))
        
        if len(data) > 0:
            short_sdf = spark.createDataFrame(data)
            short_sdf.write.mode('append').format('csv').save(host+short_interests_table_path, header=True)
    logger.warn("done!")

pull_short_interests('FNSQ', DB_HOST, TABLE_STOCK_INFO_NASDAQ, TABLE_SHORT_INTERESTS_NASDAQ)
pull_short_interests('FNYX', DB_HOST, TABLE_STOCK_INFO_NYSE, TABLE_SHORT_INTERESTS_NYSE)