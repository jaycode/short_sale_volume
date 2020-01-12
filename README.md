# Effect of Short Interest to Stock Pricing

Effect of short interest to stock pricing. Uses data from Quandl + QuoteMedia.

## Overview

It has been commonly understood that a sizeable short interest may cause a stock's price to explode higher. How common is this knowledge, though? In this project, we create a data pipeline to periodically pull short interest data and stock pricing data and store them in an S3 server for future analysis.

The data pipeline is built on Apache Airflow, with Apache Spark + Hadoop plugin to pull the data and store them in a data lake on an S3 server (with Parquet format).

## Steps

The pipeline consists of following steps:

1. Pull data from Quandl, store as Parquet.
2. Pull data from QuoteMedia, store as Parquet
3. Combine them with schema provided in the "Schema" section of this document.

The pipeline is to be run once a day, after the market closes.

## Other Scenarios

### 1. What if the data was increased by 100x?


### 2. What if we need to run the pipeline on a daily basis by 7 am every day?

### 3. What to do if the database needs to be accessed by 100+ people?
The answer to this comes in two flavors:
- If the users need to flexibly access the database to perform any SQL queries, we opt for Redshift cluster with auto-scaling capabilites.
- If the users would run basically a few sets of queries, use the combination of Apache Spark and Apache Cassandra to use the latter as a storage layer. [Here](https://opencredo.com/blogs/data-analytics-using-cassandra-and-spark/) is a link to a tutorial on this.


## Reasonings Behind Technologies Used