from py4j.protocol import Py4JJavaError

sc = spark.sparkContext
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)

def delete_path(spark, host, path):
    sc = spark.sparkContext
    java_import(sc._gateway.jvm, "java.net.URI")
    uri = sc._gateway.jvm.java.net.URI
    fs = (sc._jvm.org
          .apache.hadoop
          .fs.FileSystem
          .get(uri(host), sc._jsc.hadoopConfiguration())
          )
    fs.delete(sc._jvm.org.apache.hadoop.fs.Path(host+path), True)


def spark_table_exists(host, table_path):
    URI           = sc._gateway.jvm.java.net.URI
    Path          = sc._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem    = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    # Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration
    Configuration = sc._jsc.hadoopConfiguration


    fs = FileSystem.get(URI(host), Configuration())

    try:
        status = fs.listStatus(Path(table_path))

        return True
    except Py4JJavaError as e:
        if 'FileNotFoundException' in str(e):
            return False
        else:
            print(e)


def check_basic_quality(logger, host, table_path, table_type='parquet'):
    """ Checks quality of DAG.
    
    We do this by checking if the table exists and is not empty.
    
    Args:
        - table_type(str): 'parquet' or 'csv'
    """
    if not spark_table_exists(host, table_path):
        logger.warn("(FAIL) Table {} does not exist".format(host+table_path))
    else:
        if table_type == 'parquet':
            count = spark.read.parquet(host+table_path).count()
        elif table_type == 'csv':
            count = spark.read.csv(host+table_path, header=True).count()
            
        if count == 0:
            logger.warn("(FAIL) Table {} is empty.".format(host+table_path))
        else:
            logger.warn("(SUCCESS) Table {} has {} rows.".format(host+table_path, count))
