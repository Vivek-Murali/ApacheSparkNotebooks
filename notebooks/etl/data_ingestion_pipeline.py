"""
Sample ETL Pipeline for Data Ingestion
This notebook demonstrates a basic ETL pipeline using PySpark and Delta Lake.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta.tables import DeltaTable
import sys
import os

# Add src directory to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src')))

from config.databricks_config import STORAGE_CONFIG

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("ETL Pipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

def read_source_data(source_path):
    """
    Read data from source
    """
    return spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(source_path)

def transform_data(df):
    """
    Apply transformations to the data
    """
    return df \
        .withColumn("processed_timestamp", current_timestamp()) \
        .withColumn("year", year(col("date"))) \
        .withColumn("month", month(col("date")))

def write_to_delta(df, table_name):
    """
    Write data to Delta Lake
    """
    delta_path = f"{STORAGE_CONFIG['delta_lake_path']}/{table_name}"
    
    df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(delta_path)

def main():
    # Example usage
    source_path = "path/to/your/source/data.csv"
    
    # Read data
    df = read_source_data(source_path)
    
    # Transform data
    transformed_df = transform_data(df)
    
    # Write to Delta Lake
    write_to_delta(transformed_df, "processed_data")
    
    # Create table in the metastore
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS processed_data
        USING DELTA
        LOCATION '{STORAGE_CONFIG['delta_lake_path']}/processed_data'
    """)

if __name__ == "__main__":
    main() 