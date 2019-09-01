# Summary of the project
This project builds an ETL pipeline for a database hosted on Redshift by firstly loading data from S3 to staging tables on Redshift and execute SQL statements that create the Star schema analytics tables from these staging tables.

# Files in the repository

### create_tables.py：reset the database before running ETL pipeline

* import configparser ## to read the configuration file
* import psycopg2
* from sql_queries import create_table_queries, drop_table_queries
* Connect to the database (guided by dwh.cfg), drop all tables if exist and create tables

### sql_queries.py: includes all queries

* import configparser
* config = configparser.ConfigParser()
* config.read('dwh.cfg')
* DROP staging tables and analytics tables
* CREATE staging tables and analytics tables
* COPY staging tables
* CAUTION: CREDENTIALS 'aws_iam_role={}'----NO SPACE ON BOTH SIDES OF ‘=’
* INSERT to analytics tables
* Other queries
* Make query lists for create_tables.py and etl.py

### etl.py: script of the ETL process

* import configparser and psycopg2
* from sql_queries import copy_table_queries, insert_table_queries
* Extract JSON data from S3 and load as staging tables on Redshift by executing COPY statements 
* Load data from staging tables to analytics tables on Redshift by executing INSERT statements

### dwh.cfg: includes configuration and connection parameters of the AWS cluster

* host (i.e., endpoint), db_name, db_user, db_password, db_port

# To run the Python scripts

### STEP 1: Create an IAM role and attaching policy on AWS console

* The cluster itself needs to access (real-only at least) an S3 bucket, so we need to impersonate the cluster using IAM role configuration

### STEP 2: Create Security Group on AWS console

* Open an incoming TCP port to access the cluster endpoint:
* Because we need to access it from outside

### STEP 3: Launch a Redshift cluster

* By default, the cluster is accessible only from the virtual private cloud
* Instead, create a Redshift cluster with boto3
* Quick launch cluster – Switch to advanced settings
* Make sure the master username is identical to the IAM user defined later in the following step

### STEP 4: Create an IAM User

* Sign in to the AWS Management Console and open the IAM console.
* In the left navigation pane, choose Users. Choose Add User.

### STEP 5: In your own command prompt:

* Run create_tables.py
* Run etl.py

### STEP 6: Delete the redshift cluster on AWS console when finished

* Clusters – cluster – Delete cluster
