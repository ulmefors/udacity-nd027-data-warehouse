# udacity-nd027-data-warehouse

Project submission for Udacity Data Engineering Nanodegree - Data Warehouse

## Summary
This project combines song listen log files with song metadata to facilitate analytics. A Redshift cluster is created using the Python SDK and a data pipeline built in Python and SQL prepares a data schema designed for analytics. JSON data is copied from an S3 bucket to Redshift staging tables before being inserted into a star schema with fact and dimension tables. Analytics queries on the ` songplays` fact table are straightforward, and additional fields can be easily accessed in the four dimension tables `users`, `songs`, `artists`, and `time`. A star schema is suitable for this application since denormalization is easy, queries can be kept simple, and aggregations are fast.

## Install

```bash
$ pip install -r requirements.txt
```

## Files

**`create_cluster.py`**

* Create IAM role, Redshift cluster, and allow TCP connection from outside VPC
* Pass `--delete` flag to delete resources

**`create_tables.py`** Drop and recreate tables

**`dwh.cfg`** Configure Redshift cluster and data import

**`etl.py`** Copy data to staging tables and insert into star schema fact and dimension tables

**`sql_queries.py`**

* Creating and dropping staging and star schema tables
* Copy JSON data from S3 to Redshift staging tables
* Insert data from staging tables to star schema fact and dimension tables

## Run scripts

Set environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.

Choose `DB/DB_PASSWORD` in `dhw.cfg`.

Create IAM role, Redshift cluster, and configure TCP connectivity

```bash
$ python create_cluster.py
```

Complete `dwh.cfg` with outputs from `create_cluster.py`
* `CLUSTER/HOST`
* `IAM_ROLE/ARN`

Drop and recreate tables

```bash
$ python create_tables.py
```

Run ETL pipeline

```bash
$ python etl.py
```

Delete IAM role and Redshift cluster
```bash
$ python create_cluster.py --delete
```

## Further work

* Add data quality checks
* Create a dashboard for analytic queries on new database