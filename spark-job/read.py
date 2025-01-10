import os
import uuid
import sys
import boto3
import time
import json
import logging
from urllib.parse import urlparse
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import input_file_name, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, FloatType, DoubleType, \
    BooleanType, TimestampType, DateType
from pyspark.sql.avro.functions import from_avro, to_avro

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger('ETL_Process')


class Poller:
    def __init__(self, queue_url):
        self.queue_url = queue_url
        self.sqs_client = boto3.client('sqs')
        self.batch_size = 10
        self.messages_to_delete = []

    def get_messages(self, batch_size):
        logger.info("Polling messages from SQS queue.")
        response = self.sqs_client.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=batch_size,
            WaitTimeSeconds=20
        )

        if 'Messages' in response:
            messages = response['Messages']
            for message in messages:
                self.messages_to_delete.append({
                    'ReceiptHandle': message['ReceiptHandle'],
                    'Body': message['Body']
                })
            logger.info(f"Retrieved {len(messages)} messages from the queue.")
            return messages
        else:
            logger.info("No messages retrieved from the queue.")
            return []

    def commit(self):
        for message in self.messages_to_delete:
            logger.info(f"Deleting message: {message['Body']}")
            self.sqs_client.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=message['ReceiptHandle']
            )
        logger.info(f"Deleted {len(self.messages_to_delete)} messages from the queue.")
        self.messages_to_delete = []


def create_spark_session(spark_config, protocol="s3"):
    builder = SparkSession.builder

    if protocol == "s3a":
        default = {
            "spark.hadoop.fs.s3a.access.key": os.getenv("AWS_ACCESS_KEY_ID", ""),
            "spark.hadoop.fs.s3a.secret.key": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
            "spark.sql.catalog.dev.s3.endpoint": "https://s3.amazonaws.com",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        }
        for key, value in default.items():
            builder = builder.config(key, value)

    for key, value in spark_config.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def read_csv(spark, files, input_config):
    df = spark.read.options(**input_config.get("csv_options")).csv(files)
    return df


class AvroSchema(object):
    def read_schema(self, path, spark):
        logger.info(f"Reading Avro schema from path: {path}")
        parsed_url = urlparse(path)
        if parsed_url.scheme in ['s3', 's3a']:
            schema_json = self._read_s3_file_schema(parsed_url)
        else:
            with open(path, 'r') as f:
                schema_json = f.read()

        avro_schema = json.loads(schema_json)
        spark_schema = self._avro_to_spark_schema(avro_schema)
        logger.info("Avro schema successfully converted to Spark schema")
        return spark.createDataFrame([], schema=spark_schema)

    def _read_s3_file_schema(self, parsed_url):
        logger.info(f"Reading schema from S3: {parsed_url.netloc}/{parsed_url.path}")
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=parsed_url.netloc, Key=parsed_url.path.lstrip('/'))
        return response['Body'].read().decode('utf-8')

    def _avro_to_spark_schema(self, avro_schema):
        logger.info("Converting Avro schema to Spark schema")

        def convert_type(avro_type):
            type_mapping = {
                'string': StringType(),
                'int': IntegerType(),
                'long': LongType(),
                'float': FloatType(),
                'double': DoubleType(),
                'boolean': BooleanType(),
                'timestamp-micros': TimestampType(),
                'date': DateType()
            }
            if isinstance(avro_type, dict):
                if avro_type.get('logicalType') == 'timestamp-micros':
                    return TimestampType()
                elif avro_type.get('logicalType') == 'date':
                    return DateType()
            return type_mapping.get(avro_type, StringType())

        fields = []
        for field in avro_schema['fields']:
            field_type = field['type']
            if isinstance(field_type, dict):
                spark_type = convert_type(field_type)
                nullable = True
            elif isinstance(field_type, list):
                non_null_type = next(t for t in field_type if t != 'null')
                spark_type = convert_type(non_null_type)
                nullable = 'null' in field_type
            else:
                spark_type = convert_type(field_type)
                nullable = False
            fields.append(StructField(field['name'], spark_type, nullable))
        logger.info(f"Created {len(fields)} fields for Spark schema")
        return StructType(fields)


class MergeQuery:
    def __init__(self, merge_query_path):
        self.merge_query_path = merge_query_path
        self.parsed_url = urlparse(merge_query_path)
        logger.info(f"MergeQuery initialized with path: {merge_query_path}")

    def read_merge_query(self):
        logger.info(f"Reading merge query from: {self.merge_query_path}")
        if self.parsed_url.scheme in ['s3', 's3a']:
            return self._read_s3_file()
        else:
            return self._read_local_file()

    def _read_s3_file(self):
        logger.info(f"Reading merge query from S3: {self.parsed_url.netloc}/{self.parsed_url.path}")
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=self.parsed_url.netloc, Key=self.parsed_url.path.lstrip('/'))
        return response['Body'].read().decode('utf-8')

    def _read_local_file(self):
        logger.info(f"Reading merge query from local file: {self.merge_query_path}")
        with open(self.merge_query_path, 'r') as f:
            return f.read()

    def execute_merge(self, spark, df):
        logger.info("Executing merge query")
        merge_query = self.read_merge_query()
        df.createOrReplaceTempView("source_table")
        spark.sql(merge_query)
        logger.info("Merge query executed successfully")
        spark.catalog.dropTempView("source_table")
        logger.info("Temporary view 'source_table' dropped")

class TargetIcebergTableBuckets( AvroSchema, MergeQuery):
    def __init__(self, output_config):
        AvroSchema.__init__(self)
        MergeQuery.__init__(self, merge_query_path=output_config.get("merge_query"))
        self.output_config = output_config
        self.table_name = output_config['table_name']
        self.mode = output_config.get('mode', 'append')
        self.schema = output_config['schema']
        self.table_type = output_config.get('table_type', 'COW')
        self.compression = output_config.get('compression', 'zstd')
        self.partition = output_config.get('partition', '')
        logger.info(f"TargetIcebergTableBuckets initialized with table: {self.table_name}, mode: {self.mode}")

    def write(self, df, spark):
        full_table_name = f"{self.output_config['catalog_name']}.{self.output_config['database']}.{self.output_config['table_name']}"
        logger.info(f"Writing to Iceberg table: {full_table_name}")

        table_exists = spark.catalog.tableExists(full_table_name)
        if not table_exists:
            logger.info(f"Table {full_table_name} does not exist. Creating new table with provided schema.")
            empty_df = self.read_schema(
                path=self.output_config.get("schema"),
                spark=spark
            )
            logger.debug(f"Empty DataFrame schema: {empty_df.schema}")

            table_properties = {
                'write.delete.mode': 'merge-on-read',
                'write.update.mode': 'merge-on-read',
                'write.merge.mode': 'merge-on-read'
            } if self.table_type.upper() == 'MOR' else {
                'write.delete.mode': 'copy-on-write',
                'write.update.mode': 'copy-on-write',
                'write.merge.mode': 'copy-on-write'
            }

            writer = empty_df.writeTo(full_table_name).using("iceberg").tableProperty("format-version", "2")
            for key, value in table_properties.items():
                writer = writer.tableProperty(key, value)
                logger.debug(f"Setting table property: {key}={value}")

            if self.partition:
                writer = writer.partitionedBy(self.partition)
                logger.info(f"Partitioning table by: {self.partition}")

            writer.create()
            logger.info(f"Created Iceberg Table: {full_table_name}")

        try:
            if self.mode == 'append':
                logger.info("Appending data to table")
                df.write.format("iceberg").mode("append").saveAsTable(full_table_name)
            elif self.mode == 'overwrite':
                logger.info("Overwriting table data")
                df.write.format("iceberg").mode("overwrite").saveAsTable(full_table_name)
            elif self.mode == 'merge':
                logger.info("Merging data into table")
                self.execute_merge(spark, df)
            else:
                raise ValueError(f"Unsupported write mode: {self.mode}")

            logger.info("Data written successfully to Iceberg table")
        except Exception as e:
            logger.error(f"Failed to write to Iceberg table: {str(e)}")
            raise




def process_message(messages, spark, input_config, output_config):
    try:
        batch_files = []

        for message in messages:
            payload = json.loads(message['Body'])
            records = payload['Records']
            protocol = input_config.get("protocol", "s3")

            if protocol == "s3a":
                s3_files = [f"s3a://{record['s3']['bucket']['name']}/{record['s3']['object']['key']}" for record in
                            records]
            else:
                s3_files = [f"s3://{record['s3']['bucket']['name']}/{record['s3']['object']['key']}" for record in
                            records]

            batch_files.extend(s3_files)
        print("batch_files")
        print(batch_files)

        if batch_files:
            if input_config.get("format") == "csv":
                logger.info(f"Processing {len(batch_files)} files with protocol {protocol}")
                df = read_csv(spark=spark, files=batch_files, input_config=input_config)
                df.show()
                logger.info("Creating target unmanaged_iceberg ")
                target = TargetIcebergTableBuckets(output_config=output_config)
                target.write(df, spark)
    except Exception as e:
        logger.error(f"Error processing message: {str(e)}")
        raise Exception("Error Processing Message")

def load_config(config_path):
    parsed_url = urlparse(config_path)

    if parsed_url.scheme in ['s3', 's3a']:
        # S3 path
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=parsed_url.netloc, Key=parsed_url.path.lstrip('/'))
        config_content = response['Body'].read().decode('utf-8')
    else:
        # Local path
        with open(config_path, 'r', encoding='utf-8') as f:
            config_content = f.read()

    return json.loads(config_content)

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Run ETL process")
    parser.add_argument("--configfile", required=True, help="Path to the configuration file")
    args = parser.parse_args()

    config_path = args.configfile
    print("config_path")
    print(config_path)
    if not config_path:
        logger.error("Config file path not provided. Please use --configfile <PATH>")
        sys.exit(1)

    logger.info(f"Loading configuration from: {config_path}")
    config = load_config(config_path)

    logger.info("Configuration loaded successfully")
    logger.debug(f"Configuration: {json.dumps(config, indent=3)}")
    event =config

    # Get poll interval from config or use default
    poll_interval = int(event.get("input_config", {}).get("poll_interval", 120))
    logger.info(f"Using poll interval of {poll_interval} seconds")

    spark = create_spark_session(
        event.get("spark"),
        protocol=event.get("input_config").get("protocol", "s3")
    )

    poller = Poller(event.get("input_config").get("queue_url"))
    commit_checkpoint = event.get("input_config").get("commit_checkpoint", True)

    while True:
        try:
            messages = poller.get_messages(poller.batch_size)
            if not messages:
                logger.info("No messages to process.")
            else:
                process_message(messages=messages,
                                spark=spark,
                                input_config=event.get("input_config"),
                                output_config=event.get("output_config"),
                                )

            if commit_checkpoint:
                poller.commit()

            logger.info(f"Waiting {poll_interval} seconds before next poll")
            time.sleep(poll_interval)

        except Exception as e:
            logger.error(f"Error in polling loop: {str(e)}")
            time.sleep(poll_interval)


if __name__ == "__main__":
    main()
