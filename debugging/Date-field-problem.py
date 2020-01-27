short_sdf = spark.read \
    .csv('s3a://short-interest-effect/data/raw/short_interests_nasdaq', header=True)

symbol = 'TXG'
dates = short_sdf.select('Date').where(short_sdf.Symbol == F.lit(symbol)) \
    .orderBy(F.desc('Date')).take(1)
    
print(dates)
