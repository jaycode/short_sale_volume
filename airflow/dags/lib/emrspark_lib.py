import boto3
from pyspark.sql import SparkSession


def create_cluster():
    emr_master_sg = get_security_group()
    return conn

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.acl.default", "BucketOwnerFullControl")
    
    return spark

def write_to_s3():
    pass


def run_spark_job(master_dns):
    response = spark_submit(master_dns)
    track_statement_progress(master_dns, response)

    
def spark_submit():
    host = 'http://' + master_dns + ':8999'
    data = {'className': "com.app.RunBatchJob",
            'conf':{"spark.hadoop.fs.s3a.impl":"org.apache.hadoop.fs.s3a.S3AFileSystem"},
            'file': "s3a://your_bucket/spark_batch_job-1.0-SNAPSHOT-shadow.jar"}
    headers = {'Content-Type': 'application/json'}
    response = requests.post(host + '/batches', data=json.dumps(data), headers=headers)
    

def get_security_group(group_name, region_name):
    ec2 = boto3.client('ec2', region_name=region_name)
    response = ec2.describe_security_groups(GroupNames=[group_name])
    return response['SecurityGroups'][0]['GroupId']