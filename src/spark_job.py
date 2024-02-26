from datetime import datetime
import uuid
import os
import sys
PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, date_format, lit

from helpers.utils import METADATA_SCHEMA, csv_has_header


# Initialize Spark session with Delta support
spark = SparkSession.builder \
    .appName("Ingest CSV to DeltaLake") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "./Logs/spark-logs") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.2.1") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .enableHiveSupport() \
    .getOrCreate()


def schema_exists(metadata_catalog_df, column_names, data_types):
    """
    Check if a schema already exists in the metadata catalog.

    Parameters:
    - metadata_catalog_df (DataFrame): The DataFrame containing the metadata catalog.
    - column_names (List[str]): List of column names in the DataFrame being ingested.
    - data_types (List[str]): List of data types corresponding to column_names.

    Returns:
    - bool: True if the schema exists, False otherwise.
    """
    for row in metadata_catalog_df.collect():
        if row['column_names'] == column_names and row['data_types'] == data_types:
            return True
    return False


def ingest_meta_data(df, csv_file_path, metadata_catalog_path):
    """
    Generate metadata for a given DataFrame and CSV file, checks if the schema already exists to avoid duplication
    and append it to a metadata catalog.

        Parameters:
        - df (DataFrame): The DataFrame for which metadata is being generated.
        - csv_file_path (str): The path to the CSV file being ingested.
        - metadata_catalog_path (str): The Delta table path where metadata is stored.

        Returns:
        - str: The unique schema ID generated for this ingestion.
    """
    # Attempt to read the existing metadata catalog && Check if metadata_catalog is already created
    if os.listdir(metadata_catalog_path):
        try:
            metadata_catalog_df = spark.read.format("delta").load(metadata_catalog_path)
            if schema_exists(metadata_catalog_df, df.columns,
                             [field.dataType.simpleString() for field in df.schema.fields]):
                return [row['schema_id'] for row in metadata_catalog_df.collect() if row['column_names'] == df.columns][0]
        except Exception as e:
            print(f"Metadata catalog read error: {e}")
    # Generate metadata
    file_size = os.path.getsize(csv_file_path)
    row_count = df.count()
    schema_id = str(uuid.uuid4())
    column_names = df.columns
    data_types = [field.dataType.simpleString() for field in df.schema.fields]
    ingestion_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # Append metadata to catalog
    metadata_df = spark.createDataFrame(
        [(schema_id, column_names, data_types, ingestion_time, csv_file_path, file_size, row_count)],
        schema=METADATA_SCHEMA)
    metadata_df.write.mode("append").format("delta").save(metadata_catalog_path)
    return schema_id


def ingest_csv_to_delta(folder_path, delta_table_path, metadata_catalog_path='delta_tables/metadata_catalog'):
    """
        Ingest CSV files from a specified folder into a Delta table and catalog metadata for each file.

        Parameters:
        - folder_path (str): The folder path containing CSV files to be ingested.
        - delta_table_path (str): The Delta table path where data is stored.
        - metadata_catalog_path (str): The Delta table path for storing metadata.
    """
    csv_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.csv')]
    print(f"All CSV files found to ingest:\t{csv_files}")
    for file_path in csv_files:
        try:
            df = spark.read.option("inferSchema", "true") \
                .option("header", csv_has_header(file_path=file_path)).csv(file_path)
            df.show()
        except Exception as e:
            print(f"Error reading file {file_path}: {e}")
        schema_id = ingest_meta_data(df=df, csv_file_path=file_path, metadata_catalog_path=metadata_catalog_path)
        # Generate UUID for batch
        batch_id = str(uuid.uuid4())
        # Add ingestion_tms, batch_id and schema_id
        df = df.withColumn("ingestion_tms", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss")) \
            .withColumn("batch_id", lit(batch_id)).withColumn("schema_id", lit(schema_id))
        df.show()
        # Write to DeltaLake in APPEND mode
        df.write.format("delta").mode("append").option("mergeSchema", "true").save(delta_table_path)


def main(folder_path, delta_table_path, metadata_catalog_path):
    ingest_csv_to_delta(folder_path=folder_path, delta_table_path=delta_table_path,
                        metadata_catalog_path=metadata_catalog_path)
    # Read all data from DeltaLake datalake
    df_delta = spark.read.format("delta").load(delta_table_path)
    df_delta.show()
    # Read all data from DeltaLake metadata
    df_delta = spark.read.format("delta").load(metadata_catalog_path)
    df_delta.show()


if __name__ == '__main__':
    main(folder_path="data/sample_data/",
         delta_table_path="delta_tables/data_lake",
         metadata_catalog_path="delta_tables/metadata_catalog")
