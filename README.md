# s3-sqs-data-ingestion-demo
Learn How to Ingest Data Incrementally from S3 Using S3 Events and SQS into New S3 table Buckets | Run it Locally | Hands on 
![image](https://github.com/user-attachments/assets/6764bf37-d5de-4a1f-9631-7678dec9cb67)


# job
```
{
  "spark": {
    "spark.app.name": "iceberg_lab",
    "spark.jars.packages": "com.amazonaws:aws-java-sdk-bundle:1.12.661,org.apache.hadoop:hadoop-aws:3.3.4,software.amazon.awssdk:bundle:2.29.38,com.github.ben-manes.caffeine:caffeine:3.1.8,org.apache.commons:commons-configuration2:2.11.0,software.amazon.s3tables:s3-tables-catalog-for-iceberg:0.1.3,org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.6.1",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.defaultCatalog": "s3tablesbucket",
    "spark.sql.catalog.s3tablesbucket": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.s3tablesbucket.catalog-impl": "software.amazon.s3tables.iceberg.S3TablesCatalog",
    "spark.sql.catalog.s3tablesbucket.warehouse": "XXX",
    "spark.sql.catalog.s3tablesbucket.client.region": "us-east-1"

  },
  "input_config": {
    "queue_url": "https://sqs.us-east-1.amazonaws.com/XXX/data-processing-queue",
    "poll_interval": "60",
    "protocol": "s3a",
    "type": "sqs",
    "format": "csv",
    "transform_query": "",
    "commit_checkpoint": true,
    "csv_options": {
      "sep": "\t",
      "header": "true",
      "inferSchema": "true"
    }
  },
  "output_config": {
    "catalog_name": "s3tablesbucket",
    "database": "example_namespace",
    "table_name": "orders",
    "type": "unmanaged_iceberg",
    "mode": "merge",
    "schema": "/Users/sshah/IdeaProjects/zh-lakehouse-etl/sqs/spark-job/silver_orders.avsc",
    "merge_query": "/Users/sshah/IdeaProjects/zh-lakehouse-etl/sqs/spark-job/mergeSQL.sql",
    "table_type": "COW",
    "compression": "zstd",
    "partition": "destinationstate"
  }
}
```

# Run it 
```
export AWS_ACCESS_KEY_ID="XXXX"
export AWS_SECRET_ACCESS_KEY="XX"
export BUCKET="XXX"

python3 \
    /Users/sshah/IdeaProjects/zh-lakehouse-etl/sqs/reader/read.py \
   --configfile /Users/sshah/IdeaProjects/zh-lakehouse-etl/sqs/reader/job.json

```
