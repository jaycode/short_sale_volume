from airflow.configuration import conf as airflow_config
import configparser
import json
import os

config = configparser.ConfigParser()
airflow_dir = os.path.split(airflow_config['core']['dags_folder'])[0]
config.read('{}/config.cfg'.format(airflow_dir))

CLUSTER_NAME = config['AWS']['CLUSTER_NAME']
VPC_ID = config['AWS']['VPC_ID']
SUBNET_ID = config['AWS']['SUBNET_ID']

if config['App']['STOCKS'] == '':
    STOCKS = []
else:
    STOCKS = json.loads(config.get('App', 'STOCKS').replace("'", '"'))

if config['App']['STOCK_LIMITS'] == '':
    LIMIT = None
else:
    LIMIT = int(config['App']['STOCK_LIMITS'])
