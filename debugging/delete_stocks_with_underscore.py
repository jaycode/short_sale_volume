# Previously the stocks with underscore were not downloaded properly. We delete from our database here.

from pyspark.sql.utils import AnalysisException

def perform_delete(table_name):
    logger.warn("Table to process: {}".format(table_name))
    try:
        sdf = spark.read \
            .csv(table_name, header=True)
    except AnalysisException:
        raise ValueError("Table {} is empty".format(table_name))

    rows = sdf.where(F.col('Symbol').contains('_')).collect()

    total = len(rows)
    logger.warn("BEFORE: Number of rows for that contains underscore: {}".format(total))

    if total > 0:
        sdf = spark.read.csv(table_name, header=True)
        sdf = sdf.where(~F.col('Symbol').contains('_'))
        sdf.write.mode('overwrite').format('csv').save(table_name, header=True)

        # Testing:
        sdf = spark.read \
            .csv(table_name, header=True)

        rows = sdf.where(F.col('Symbol').contains('_')).collect()

        total = len(rows)
        print("AFTER: Number of rows for that contains underscore: {}".format(total))


perform_delete(DB_HOST+TABLE_SHORT_INTERESTS_NASDAQ)
perform_delete(DB_HOST+TABLE_SHORT_INTERESTS_NYSE)