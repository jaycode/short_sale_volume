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
    - NASDAQ: https://old.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nasdaq&render=download
    - NYSE: https://old.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nyse&render=download
3. Short Interest DAG: Pull short interest data from [Quandl's Financial Industry Regulatory Authority's short interest data](https://www.quandl.com/data/FINRA-Financial-Industry-Regulatory-Authority). Store the data in S3 server (or locally, depending on the setting in `config.cfg`).
4. Prices DAG: Pull pricing data from QuoteMedia, store as CSV.
5. Combine DAG: Combine the short interests and stock price data.
6. Once the data are combined, Cluster DAG continues to terminate the EMR cluster.

The pipeline is to be run once a day at 00:00. On the first run, it gets all data up to yesterday's date. In the following dates, we get one day of data for each day.


## How to setup and run

1. Create a key pair in AWS EC2 console.
2. Create a CloudFormation stack from `basic_network.yml` template. This is a generic VPC configuration with 2 private and 2 public subnets which should be quite useful for other similar projects too. I recommend setting the stack name with something generic, like "BasicNetwork".
3. Create a CloudFormation stack from `aws-cf_template.yml`. Pass in your Quandl key and AWS Access ID and Private Access Key. I would name this stack with a specific project's name like "ShortInterests".
4. After the stack created, go to the "Outputs" tab to get the URL of the Airflow admin, something like `http://ec2-3-219-234-248.compute-1.amazonaws.com:8080`. You can get the endpoint from there, which you can use to SSH connect.
    ```
    chmod 400 ~/path/to/airflow_pem.pem
    ssh -i "~/path/to/airflow_pem.pem" ec2-user@ec2-3-219-234-248.compute-1.amazonaws.com
    ```

5. Connect via ssh to the server. Note that the Airflow admin may likely not to be ready just yet, because there may be some code that are still running on the EC2 server. To check on the progress, SSH connect to the EC2 instance, then run this command `cat /var/log/user-data.log` to see the entire log, or `tail /var/log/user-data.log` to view the last few lines.
6. Once the Airflow webserver is ready (check the file `/var/log/user-data.log` a few times until there are no changes), run the following commands on the server to run Airflow Scheduler:

    ```
    source ~/.bashrc
    airflow scheduler -D
    aws configure
    ```

    Then enter your AWS credentials and default region. If you got the following error:

    ```
    Warning: You have two airflow.cfg files: /home/ec2-user/airflow/airflow.cfg and /home/ec2-user/short_interest_effect/airflow/airflow.cfg. Airflow used to look at ~/airflow/airflow.cfg, even when AIRFLOW_HOME was set to a different value. Airflow will now only read /home/ec2-user/short_interest_effect/airflow/airflow.cfg, and you should remove the other file
    ```

    Run this code:
    ```
    rm -r airflow 
    ```

    Before running the scheduler.


To kill Airflow scheduler:

```
$ kill $(ps -ef | grep "airflow scheduler" | awk '{print $2}')
```

## Other Scenarios

### 1. What if the data were increased by 100x?
The main problem is with pulling the data from the APIs. Each request takes about 3 seconds. To make this process faster, we can try adding more nodes in our EM3 cluster to deal with this situation.

### 2. What if we needed to run the pipeline on a daily basis by 7 am every day?
Just need to update `schedule_interval` setting accordingly for all of the DAGs for this.

### 3. What to do if the database needed to be accessed by 100+ people?
The answer to this comes in two flavors:
- If the users need to flexibly access the database to perform any SQL queries, we opt for Redshift cluster with auto-scaling capabilites.
- If the users would run basically a few sets of queries, use the combination of Apache Spark and Apache Cassandra to use the latter as a storage layer. [Here](https://opencredo.com/blogs/data-analytics-using-cassandra-and-spark/) is a link to a tutorial on this.


## Reasonings Behind Technologies Used

- Amazon EMR: Easy to scale up or shrink down, and we do not need to keep the server running all the time.
- Apache Airflow: Scheduler application. Another interesting alternative for this is AWS Step Functions, but for
  this project Apache Airflow is preferable as it is way easier to setup.
- Apache Spark: Good for working with huge datasets.
