from py4j.protocol import Py4JJavaError
from pyspark.sql.utils import AnalysisException


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


def copyMerge(spark, host, src_dir, dst_file, overwrite=False, deleteSource=False, debug=False):
    
    sc = spark.sparkContext
    
    hadoop = sc._jvm.org.apache.hadoop
#     conf = hadoop.conf.Configuration()
    conf = sc._jsc.hadoopConfiguration()
#     fs = hadoop.fs.FileSystem.get(conf)
    java_import(sc._gateway.jvm, "java.net.URI")
    uri = sc._gateway.jvm.java.net.URI
    fs = (sc._jvm.org
          .apache.hadoop
          .fs.FileSystem
          .get(uri(host), sc._jsc.hadoopConfiguration())
          )

    # check files that will be merged
    files = []
    for f in fs.listStatus(hadoop.fs.Path(src_dir)):
        if f.isFile():
            files.append(f.getPath())
    if not files:
        raise ValueError("Source directory {} is empty".format(src_dir))
    files.sort(key=lambda f: str(f))

    # dst_permission = hadoop.fs.permission.FsPermission.valueOf(permission)      # , permission='-rw-r-----'
    out_stream = fs.create(hadoop.fs.Path(dst_file), overwrite)

    try:
        # loop over files in alphabetical order and append them one by one to the target file
        for file in files:
            if debug: 
                print("Appending file {} into {}".format(file, dst_file))

            in_stream = fs.open(file)   # InputStream object
            try:
                hadoop.io.IOUtils.copyBytes(in_stream, out_stream, conf, False)     # False means don't close out_stream
            finally:
                in_stream.close()
    finally:
        out_stream.close()

    if deleteSource:
        fs.delete(hadoop.fs.Path(src_dir), True)    # True=recursive
        if debug:
            print("Source directory {} removed.".format(src_dir))


def spark_table_exists(host, table_path):
    URI           = sc._gateway.jvm.java.net.URI
    Path          = sc._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem    = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    # Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration
    Configuration = sc._jsc.hadoopConfiguration


    fs = FileSystem.get(URI(host), Configuration())

    try:
        status = fs.listStatus(Path(table_path))
        spark.read.csv(host+table_path, header=True)

        return True
    except Py4JJavaError as e:
        if 'FileNotFoundException' in str(e):
            return False
        else:
            raise
    except AnalysisException as e:
        if 'Unable to infer schema' in str(e):
            return False
        else:
            raise


def check_basic_quality(logger, host, table_path, table_type='csv'):
    """ Checks quality of DAG.
    
    We do this by checking if the table exists and is not empty.
    
    Args:
        - table_type(str): 'parquet' or 'csv'
    """
    if not spark_table_exists(host, table_path):
        logger.warn("(FAIL) Table {} does not exist".format(host+table_path))
        return None
    else:
        if table_type == 'parquet':
            sdf = spark.read.parquet(host+table_path)
            count = sdf.rdd.countApprox(timeout=1000, confidence=0.9)
        elif table_type == 'csv':
            sdf = spark.read.csv(host+table_path, header=True)
            count = sdf.rdd.countApprox(timeout=1000, confidence=0.9)
            
        if count == 0:
            logger.warn("(FAIL) Table {} is empty.".format(host+table_path))
        else:
            logger.warn("(SUCCESS) Table {} has {} rows.".format(host+table_path, count))
        return sdf
