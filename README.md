# Module 3 Homework: Data Warehousing & BigQuery Cristian Statescu

## NOTE

Below is the exact markdown (MD) file from the original repository (data-engineering-zoomcamp) with **my answers on the bottom**. Please click [here](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2026/03-data-warehouse/homework.md) to go to the actual original file.

---

In this homework we'll practice working with BigQuery and Google Cloud Storage.

When submitting your homework, you will also need to include
a link to your GitHub repository or other public code-hosting
site.

This repository should contain the code for solving the homework.

When your solution has SQL or shell commands and not code
(e.g. python files) file format, include them directly in
the README file of your repository.

## Data

For this homework we will be using the Yellow Taxi Trip Records for January 2024 - June 2024 (not the entire year of data).

Parquet Files are available from the New York City Taxi Data found here:

https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

## Loading the data

You can use the following scripts to load the data into your GCS bucket:

- Python script: [load_yellow_taxi_data.py](./load_yellow_taxi_data.py)
- Jupyter notebook with DLT: [DLT_upload_to_GCP.ipynb](./DLT_upload_to_GCP.ipynb)

You will need to generate a Service Account with GCS Admin privileges or be authenticated with the Google SDK, and update the bucket name in the script.

If you are using orchestration tools such as Kestra, Mage, Airflow, or Prefect, do not load the data into BigQuery using the orchestrator.

Make sure that all 6 files show in your GCS bucket before beginning.

Note: You will need to use the PARQUET option when creating an external table.


## BigQuery Setup

Create an external table using the Yellow Taxi Trip Records. 

I ran the following SQL query to load the data from GCS to BigQuery

```sql
CREATE OR REPLACE EXTERNAL TABLE `module-3-486600.ny_taxi_data_2024.external_yellow_tripdata_2024`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://nyc-tl-data-cs9490/yellow_tripdata_2024-*.parquet']
);
```

Create a (regular/materialized) table in BQ using the Yellow Taxi Trip Records (do not partition or cluster this table). 

I ran the following SQL query to create a regular table in BQ from the data from the external table that was just defined.

```sql
CREATE OR REPLACE TABLE `module-3-486600.ny_taxi_data_2024.yellow_tripdata_non_partitioned` AS
SELECT * FROM `module-3-486600.ny_taxi_data_2024.external_yellow_tripdata_2024`;
```


## Question 1. Counting records

What is count of records for the 2024 Yellow Taxi Data?
- 65,623
- 840,402
- 20,332,093
- 85,431,289

### Question 1 Answer

```sql
SELECT COUNT(*) 
FROM `module-3-486600.ny_taxi_data_2024.yellow_tripdata_non_partitioned`
```

**20,332,093** is the count of records for the 2024 Yellow Taxi we have loaded in.

## Question 2. Data read estimation

Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
 
What is the **estimated amount** of data that will be read when this query is executed on the External Table and the Table?

- 18.82 MB for the External Table and 47.60 MB for the Materialized Table
- 0 MB for the External Table and 155.12 MB for the Materialized Table
- 2.14 GB for the External Table and 0MB for the Materialized Table
- 0 MB for the External Table and 0MB for the Materialized Table

### Question 2 Answer

Here is the query to count the distinct number of PULocationIDs for the entire dataset on both tables (simple change the table in the FROM clause).

```sql
SELECT COUNT(DISTINCT(PULocationID))
FROM `project_id.dataset_name.table_name`
```

Once you type in the respective tables, in the bottom left of the query window you will see that the answer is:

- **0 MB for the External Table and 155.12 MB for the regular Table**


## Question 3. Understanding columnar storage

Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table.

Why are the estimated number of Bytes different?
- BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires 
reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.
- BigQuery duplicates data across multiple storage partitions, so selecting two columns instead of one requires scanning the table twice, 
doubling the estimated bytes processed.
- BigQuery automatically caches the first queried column, so adding a second column increases processing time but does not affect the estimated bytes scanned.
- When selecting multiple columns, BigQuery performs an implicit join operation between them, increasing the estimated bytes processed

### Question 3 Answer

Here are the queries I came up with in respective order:

- Query to retrieve the PULocationID from the table (not external). Estimation from BQ: This query will process 155.12 MB when run.

```sql
SELECT PULocationID
FROM `module-3-486600.ny_taxi_data_2024.yellow_tripdata_non_partitioned`
```

- Query to retrieve the PULocationID **and** DOLocationID on the same table. Estimation from BQ: This query will process 310.24 MB when run.

```sql
SELECT PULocationID, DOLocationID
FROM `module-3-486600.ny_taxi_data_2024.yellow_tripdata_non_partitioned`
```

The reason the estimated number of bytes are different is because:

- BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires 
reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

## Question 4. Counting zero fare trips

How many records have a fare_amount of 0?
- 128,210
- 546,578
- 20,188,016
- 8,333

### Question 4 Answer

Run the following query to get the result:

```sql
SELECT COUNT(*)
FROM `module-3-486600.ny_taxi_data_2024.yellow_tripdata_non_partitioned`
WHERE fare_amount = 0
```

There are **8,333** records that have a fare_amount of 0.

## Question 5. Partitioning and clustering

What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)

- Partition by tpep_dropoff_datetime and Cluster on VendorID
- Cluster on by tpep_dropoff_datetime and Cluster on VendorID
- Cluster on tpep_dropoff_datetime Partition by VendorID
- Partition by tpep_dropoff_datetime and Partition by VendorID

### Question 5 Answer

Since we are always going to filter based on tpep_dropoff_datetime, our optimized table should firstly be partitioned by tpep_dropoff_datetime in order to prune scanned data based on our WHERE clause. 

Also, since we are going to order the results by VendorID, the optimized table should also be clustered by VendorID. Partitioning can only be done by one column per table, and we are already partitioning by tpep_dropoff_datetime. Additionally, clustering by VendorID will improve any additional filtering we may do on the column, which will also help with the sorting that will occur in in our query.

Therefore, the best strategy is:

- Partition by tpep_dropoff_datetime and Cluster on VendorID

The query used to make the partitioned table:

```sql
CREATE OR REPLACE TABLE `module-3-486600.ny_taxi_data_2024.yellow_tripdata_partitioned`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `module-3-486600.ny_taxi_data_2024.yellow_tripdata_non_partitioned`;
```

## Question 6. Partition benefits

Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime
2024-03-01 and 2024-03-15 (inclusive)


Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values? 


Choose the answer which most closely matches.
 

- 12.47 MB for non-partitioned table and 326.42 MB for the partitioned table
- 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table
- 5.87 MB for non-partitioned table and 0 MB for the partitioned table
- 310.31 MB for non-partitioned table and 285.64 MB for the partitioned table

### Question 6 Answer

The query I came up with is the following:

```sql
SELECT DISTINCT(VendorID)
FROM <replace with partitioned and non-partitioned tables>
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
```

---

For the regular table, I got the following prediction from BQ: 

This query will process 310.24 MB when run.

```sql
SELECT DISTINCT(VendorID)
FROM `module-3-486600.ny_taxi_data_2024.yellow_tripdata_non_partitioned`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
```

---

For the partitioned table, I got the following prediction from BQ:

This query will process 26.84 MB when run.

```sql
SELECT DISTINCT(VendorID)
FROM `module-3-486600.ny_taxi_data_2024.yellow_tripdata_partitioned`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
```

Therefore, the answer is:

- **310.24 MB for non-partitioned table and 26.84 MB for the partitioned table**

## Question 7. External table storage

Where is the data stored in the External Table you created?

- Big Query
- Container Registry
- GCP Bucket
- Big Table

### Question 7 Answer

The data for the External Table is stored within my GCP Bucket, as external tables in BQ have the data in their original storage location. 

If you look within the details of the External Table, it will also say the Source URI(s) (Uniform Resource Identifier) as a path to my GCP Bucket with the wildcard to pull in all avaliable months of 2024 yellow taxi trip data:

```
gs://nyc-tl-data-cs9490/yellow_tripdata_2024-*.parquet
```

Therefore, the answer is:

- GCP Bucket

## Question 8. Clustering best practices

It is best practice in Big Query to always cluster your data:
- True
- False

### Question 8 Answer

It is not always best practice in Big Query to cluster data. For tables that have smaller amounts of data or don't have very large cardinality in its columns for aggregation or querying, clustering the data can end up adding significant costs as it will incur more metadata reads, as well as unnecessary metadata maintenance when new data is inserted.

The answer is:
- False

## Question 9. Understanding table scans

No Points: Write a `SELECT count(*)` query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

### Question 9 Answer

The query I wrote is the following:

```sql
SELECT COUNT(*) 
FROM `module-3-486600.ny_taxi_data_2024.yellow_tripdata_non_partitioned`
```

BQ estimates 0 bytes will be read. The reason for this is because BQ knows the number of rows within the source table itself, and so simply returns the value stored within the table's metadata as opposed to pulling in the table and doing the count itself.

This can be proven by adding a random WHERE clause:

```sql
SELECT COUNT(*) 
FROM `module-3-486600.ny_taxi_data_2024.yellow_tripdata_non_partitioned`
WHERE VendorID = 2
```

The prediction for reading this table now is that 155.12 MB will be read. This is due to the fact that the count of rows where VendorID = 2 is **NOT** stored within the table's metadata, and so BQ will actually need to load in the data and query it in order to provide us with the count.