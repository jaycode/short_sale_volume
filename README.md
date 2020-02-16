# Effect of Short Interest to Stock Pricing - for Quantopian Self-Serve Data

Download short interests from Quandl, then process it to be uploaded to Quantopian Self-Serve Data.

## Overview

It has been commonly understood that a sizeable short interest may cause a stock's price to explode higher. How common is this knowledge, though? In this project, we create a data pipeline to periodically pull short interest data and stock pricing data and store them in an S3 server for future analysis.

The data pipeline is built on Apache Airflow, with Apache Spark + Hadoop plugin to pull the data and store them in a data lake on an S3 server (with Parquet format).

At the time of writing (2020-01-15), we have 3582 stocks from NASDAQ and 3092 stocks from NYSE. The earliest date is 2013-04-01, which accounts for nearly 1700 data points (261 working days each year). That means, we have more than 10 millions of data to process at max. At max, because many of the stocks won't have all of the days' data. Some of them may no longer exist. A complete run on 2020-02-05 returned approximately 6,798,996 rows of data (counted with `rdd.countApprox` function - see `4.vaildate_completed_data.ipynb` for the code).

To demonstrate that the pipeline works, we only use a small subset of the data, consisting of only 7 stocks configured in `airflow/config.cfg` file.

## Steps

The pipeline consists of the following tasks:

1. **Cluster DAG** creates Amazon EMR server, then it waits for the other DAGs to complete.
2. If `STOCKS` configuration in `airflow/airflow.cfg` has not been filled with stock symbols, pull list of stock information from 
   old NASDAQ links for both NYSE and NASDAQ exchanges. The links as follows:
    - NASDAQ: https://old.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nasdaq&render=download
    - NYSE: https://old.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nyse&render=download
3. Short Interest DAG, we will refer to this, and other non-cluster DAGs in the future, as **worker DAGs**: 
  - Pull short interest data from [Quandl's Financial Industry Regulatory Authority's short interest data](https://www.quandl.com/data/FINRA-Financial-Industry-Regulatory-Authority).
  - Store the data in S3 server (or locally, depending on the setting in `config.cfg`).
  - Combine data from FINRA/NYSE and FINRA/NASDAQ exchanges (**Todo: There is also ORF exchange, but not sure where to get the list of stocks. Can anybody help?**)

The pipeline is to be run once a day at 00:00. On the first run, it gets all data up to yesterday's date. In the following dates, we get one day of data for each day.


## How to setup and run

1. Create a key pair in AWS EC2 console.
2. Create a CloudFormation stack from `basic_network.yml` template. This is a generic VPC configuration with 2 private and 2 public subnets which should be quite useful for other similar projects too. I recommend setting the stack name with something generic, like "BasicNetwork". Take note of the first VPC ID and SubnetID of the first public subnet.
3. Create a CloudFormation stack from `aws-cf_template.yml`. Pass in your Quandl key and AWS Access ID and Private Access Key. I would name this stack with a specific project's name like "ShortInterests".
4. After the stack created, go to the "Outputs" tab to get the URL of the Airflow admin, something like `http://ec2-3-219-234-248.compute-1.amazonaws.com:8080`. You can get the endpoint from there, which you can use to SSH connect.
    ```
    chmod 400 ~/path/to/airflow_pem.pem
    ssh -i "~/path/to/airflow_pem.pem" ec2-user@ec2-3-219-234-248.compute-1.amazonaws.com
    ```

5. Connect via ssh to the server. Note that the Airflow admin may likely not to be ready just yet, because there may be some code that are still running on the EC2 server. To check on the progress, SSH connect to the EC2 instance, then run this command `cat /var/log/user-data.log` to see the entire log, or `tail /var/log/user-data.log` to view the last few lines.
6. Once the Airflow webserver is ready (check the file `/var/log/user-data.log` a few times until there are no changes), run the following commands on the server to run Airflow Scheduler:

    ```
    airflow scheduler -D
    ```

    If you got the following error:

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

To kill Airflow webserver:

```
$ cat $AIRFLOW_HOME/airflow-webserver.pid | sudo xargs kill -9
```

## FAQs

### Where can I see the progress of the pulling process?

Click on the DAG's name, then on either Graph View or Tree View, click on the currently running task, then click on "View Log". You will need to keep refreshing and view the bottom of the page to check on the progress. **The status is updated every 5 minutes.**

Here is an example of outputs from the log for short interests:

```
20/02/04 16:27:59 WARN DAG: downloading from exchange FNSQ - 1801/3621 - total rows in this batch: 1814543
20/02/04 16:49:11 WARN DAG: downloading from exchange FNSQ - 1901/3621 - total rows in this batch: 1937516
20/02/04 17:10:13 WARN DAG: downloading from exchange FNSQ - 2001/3621 - total rows in this batch: 2046578
```

Notice that it took about 20 minutes to pull 110,000 - 120,000 data points.


### Can I resize the size of EMR? Will it affect the running speed?

Yes and No.

You may resize through the EMR cluster page, then click on Hardware. However, currently, changing it to anything above 3 nodes won't increase the speed of downloading short interest data. The bottleneck lies in the network connection between EMR cluster's master node and Quandle or QuoteMedia server.

**Todo: Parallelization of the requests to Quandl by using UDF (user-defined-function) to perform GET requests through slave nodes may improve the speed of this process. The initial version of the code is stored in `airflow/dags/etl/pull_short_interests-udf.py`. It does not currently have any sort of reporting to tell us the progress as we pull the data, and, more critically, it creates duplicates for existing data.**

As a reference, with 3-10 EMR cluster's slave nodes, it takes between 20 to 40 minutes to load about 110,000 short interest data (100 companies on a fresh start between 2013-04-01 and 2020-02-04).

### How do I debug the Short Interests DAG?

If there is an error on any of the steps in the Short Interests DAG, the system will do the following:

1. The DAG will stop running. Set the state of this worker DAG into ERROR.
2. The Cluster DAG will detect the state, then proceeds with turning off the EMR cluster (this is to avoid exorbitant charges from leaving it running).

Therefore, the final state is a SUCCESS state for the Cluster DAG and the ERROR state for the Short Interests DAG. To re-run only the step that contains the erorr, perform the following procedure:

#### 1. Delete the Cluster DAG

Click on the delete button on the right part of the Cluster DAG:

![error-1](media/error-1.png)

#### 2. Refresh the page, and then toggle ON the Cluster DAG

Click on this OFF switch to turn it on:

![error-2](media/error-2.png)

By doing so, the Cluster DAG will restart the EMR cluster and wait for the Short Interests DAG to complete (or returns another error).

#### 3. Wait until the Cluster DAG is in *waiting* state

The *waiting* state should look like so:

![error-3](media/error-3.png)

Take note of the colors and positions of the circles.

#### 4. Clear the task that contains the error

Click on the `short_interests_dag` link to open up its details, then click on "Tree View". Within it, click the red box (the task that contains the error):

![error-4](media/error-4.png)

Then click on the "Clear" button:

![error-5](media/error-5.png)

#### 5. ????

#### 6. Profit

Now we just need to wait for the Short Interests Dag to complete running.


## Troubleshootings

> botocore.exceptions.ClientError: An error occurred (ValidationException) when calling the RunJobFlow operation: Invalid InstanceProfile: EMR_EC2_DefaultRole.

It's likely `EMR_EC2_DefaultRole` instance profile is not ready yet when we create the cluster. Clear the `create_cluster_task` from `cluster_dag` and wait for it being re-run.

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

## Todos

1. There are a couple of **Todo**s above. If you feel generous, post a pull request to improve them.
2. The `aws-latest` branch is meant to gather pricing data from QuoteMedia then combine them with the short interests data from Quandl. However, the branch still currently has some bugs, and it does not make sense to combine the data for later slicing them off when preparing the data for use in Quantopian. When the need to analyze the data outside of Quantopian arises, work on this branch further.
3. The `local-only` branch is for the local version of Airflow. This was needed when the system was initially developed. I don't see how it is ever going to be needed again, but I keep this branch just in case someone may need it.
4. Both the `aws-latest` and `local-only` branches are currently not fast enough for use with large datasets. Diff the `airflow/etl/short_interests.py` with `quantopian-only` branch and move the needed updates to the other branches. Also do similar updates to the `airflow/etl/pull_prices.py` file.
5. Investigate whether paralellization of the GET requests would improve the ETL speed (see the "FAQs" section above).