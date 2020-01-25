# Effect of Short Interest to Stock Pricing

Effect of short interest to stock pricing. Uses data from Quandl + QuoteMedia.

## Overview

It has been commonly understood that a sizeable short interest may cause a stock's price to explode higher. How common is this knowledge, though? In this project, we create a data pipeline to periodically pull short interest data and stock pricing data and store them in an S3 server for future analysis.

The data pipeline is built on Apache Airflow, with Apache Spark + Hadoop plugin to pull the data and store them in a data lake on an S3 server (with Parquet format).

At the time of writing (2020-01-15), we have 3582 stocks from NASDAQ and 3092 stocks from NYSE. The earliest date is 2013-04-01, which accounts for nearly 1700 data points (261 working days each year). That means, we have more than 10 millions of data maximum. Maximum, because many of the stocks won't have all of the days' data. Some of them may no longer exist.

To demonstrate that the pipeline works, we only use a small subset of the data, consisting of only 7 stocks configured in `airflow/config.cfg` file.

## Steps

The pipeline consists of the following tasks:

1. Cluster DAG creates Amazon EMR server, then it waits for the other DAGs to complete.
2. If `STOCKS` configuration in `airflow/airflow.cfg` has not been filled with stock symbols, pull list of stock information from 
   old NASDAQ links for both NYSE and NASDAQ exchanges. The links as follows:
   - NASDAQ: **https://old.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nasdaq&render=download**
   - NYSE': **https://old.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nyse&render=download**
3. Short Interest DAG: Pull short interest data from [Quandl's Financial Industry Regulatory Authority's short interest data](https://www.quandl.com/data/FINRA-Financial-Industry-Regulatory-Authority). Store the data in S3 server (or locally, depending on the setting in `config.cfg`).
4. Prices DAG: Pull pricing data from QuoteMedia, store as CSV.
5. Combine DAG: Combine the short interests and stock price data.
6. Once the data are combined, Cluster DAG continues to terminate the EMR cluster.

The pipeline is to be run once a day at 00:00. On the first run, it gets all data up to yesterday's date. In the following dates, we get one day of data for each day.


## How to setup and run

1. Create a key pair in AWS EC2 console
2. [Launch Stack](https://console.aws.amazon.com/cloudformation/home?region=us-west-2#/stacks/new?stackName=Airflow-Blog&templateURL=https://s3.amazonaws.com/aws-bigdata-blog/artifacts/airflow.livy.emr/airflow.yaml)

1. Update `airflow.cfg` as follows: 
    - load_examples: False
    - executor: use `LocalExecutor`
    - sql_alchemy_conn: use either MySQL or PostgreSQL's connection link.
2. Copy and rename `config.cfg.default` into `config.cfg` and fill in the following information:
    - `DB_HOST`: Supports either local path or `s3a` bucket link e.g. `s3a://bucket-name`.
    - `Quandl API_KEY`: Get API key from [Quandl](https://quandl.com) and paste it here.
    - `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`: Get them from AWS, in IAM admin.
3. Run `airflow-start.sh` to turn on the Airflow server and schedulers.


## Other Scenarios

### 1. What if the data was increased by 100x?
The main problem is with pulling the data from the APIs. Each request takes about 3 seconds. To make this process faster, we can try adding more nodes in our EM3 cluster to deal with this situation.

### 2. What if we need to run the pipeline on a daily basis by 7 am every day?
Just need to update `schedule_interval` setting accordingly for all of the DAGs for this.

### 3. What to do if the database needs to be accessed by 100+ people?
The answer to this comes in two flavors:
- If the users need to flexibly access the database to perform any SQL queries, we opt for Redshift cluster with auto-scaling capabilites.
- If the users would run basically a few sets of queries, use the combination of Apache Spark and Apache Cassandra to use the latter as a storage layer. [Here](https://opencredo.com/blogs/data-analytics-using-cassandra-and-spark/) is a link to a tutorial on this.


## Reasonings Behind Technologies Used

- Amazon EMR: Easy to scale up or shrink down, and we do not need to keep the server running all the time.
- Apache Airflow: Scheduler application. Another interesting alternative for this is AWS Step Functions, but for
  this project Apache Airflow is preferable as it is way easier to setup.
- Apache Spark: Good for working with huge datasets.