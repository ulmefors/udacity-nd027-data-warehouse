# udacity-nd027-data-warehouse

Project submission for Udacity Data Engineering Nanodegree - Data Warehouse

## Summary
This project...

## Install

```bash
$ pip install boto3
```

## Run scripts

Create IAM role, Redshift cluster, and configure TCP connectivity

```bash
$ python create_cluster.py
```

Drop and recreate tables

```bash
$ python create_tables.py
```

Run ETL pipeline

```bash
$ python etl.py
```

Delete IAM role and Redshift cluster
```
$ python create_cluster.py --delete
```

## Further work

* Add data quality checks
* Create a dashboard for analytic queries on new database