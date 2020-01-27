import sys
sys.path.append("../airflow/dags/lib")
import emrspark_lib as emrs
import configparser
import time

import logging
import os
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)

config = configparser.ConfigParser()
config.read('../airflow/config.cfg')

CLUSTER_NAME = config['AWS']['CLUSTER_NAME']
VPC_ID = config['AWS']['VPC_ID']
SUBNET_ID = config['AWS']['SUBNET_ID']

if config['App']['STOCKS'] == '':
    STOCKS = []
else:
    STOCKS = json.loads(config.get('App', 'STOCKS').replace("'", '"'))

ec2, emr, iam = emrs.get_boto_clients(config['AWS']['REGION_NAME'], config=config)

if VPC_ID == '':
    VPC_ID = emrs.get_first_available_vpc(ec2)

if SUBNET_ID == '':
    SUBNET_ID = emrs.get_first_available_subnet(ec2, VPC_ID)

#####

master_sg_id = emrs.create_security_group(ec2, '{}SG'.format(CLUSTER_NAME),
    'Master SG for {}'.format(CLUSTER_NAME), VPC_ID)
slave_sg_id = emrs.create_security_group(ec2, '{}SlaveSG'.format(CLUSTER_NAME),
    'Slave SG for {}'.format(CLUSTER_NAME), VPC_ID)

keypair = emrs.recreate_key_pair(ec2, '{}_pem'.format(CLUSTER_NAME))

emrs.recreate_default_roles(iam)

cluster_id = emrs.create_emr_cluster(emr, CLUSTER_NAME,
                master_sg_id,
                slave_sg_id,
                keypair['KeyName'], SUBNET_ID,
                release_label='emr-5.28.1')
cluster_dns = emrs.get_cluster_dns(emr, cluster_id)

#####

args = {
    'AWS_ACCESS_KEY_ID': config['AWS']['AWS_ACCESS_KEY_ID'],
    'AWS_SECRET_ACCESS_KEY': config['AWS']['AWS_SECRET_ACCESS_KEY']
}

emrs.kill_all_inactive_spark_sessions(cluster_dns)
session_headers = emrs.create_spark_session(cluster_dns)
emrs.wait_for_spark(cluster_dns, session_headers)
job_response_headers = emrs.submit_spark_job_from_file(
        cluster_dns, session_headers,
        'spark_table_exists-debug.py',
        args=args,
        commonpath='airflow/dags/etl/common.py',
        helperspath='airflow/dags/etl/helpers.py'
)
final_status, logs = emrs.track_spark_job(cluster_dns, job_response_headers)
emrs.kill_spark_session(cluster_dns, session_headers)
for line in logs:
    logging.info(line)
    if '(FAIL)' in str(line):
        logging.error(line)
        raise Exception("ETL process fails.")