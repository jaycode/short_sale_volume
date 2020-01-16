import requests
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Row

from py4j.java_gateway import java_import

Logger = spark._jvm.org.apache.log4j.Logger
logger = Logger.getLogger("DAG")
spark.sparkContext.setLogLevel('WARN')
