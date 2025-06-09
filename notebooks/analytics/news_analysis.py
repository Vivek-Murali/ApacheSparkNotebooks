"""
News Data Analysis using PySpark
This notebook demonstrates analysis of news data extracted from Event Registry.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
import os
from pathlib import Path

# Add project root to Python path
project_root = str(Path(__file__).parent.parent.parent)
sys.path.append(project_root)

from data.news_extraction import extract_news_data
from src.config.databricks_config import STORAGE_CONFIG

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("News Analysis") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

def create_news_schema():
    """
    Define schema for news data
    """
    return StructType([
        StructField("uri", StringType(), True),
        StructField("title", StringType(), True),
        StructField("body", StringType(), True),
        StructField("date", TimestampType(), True),
        StructField("source", StringType(), True),
        StructField("sentiment", DoubleType(), True)
    ])

def process_news_data(articles):
    """
    Process news articles into a Spark DataFrame
    """
    # Convert articles to list of dictionaries
    news_data = []
    for article in articles:
        news_data.append({
            "uri": article.get("uri", ""),
            "title": article.get("title", ""),
            "body": article.get("body", ""),
            "date": article.get("date", ""),
            "source": article.get("source", {}).get("title", ""),
            "sentiment": article.get("sentiment", 0.0)
        })
    
    # Create DataFrame
    return spark.createDataFrame(news_data, schema=create_news_schema())

def analyze_news(df):
    """
    Perform analysis on news data
    """
    # Basic statistics
    print("Total number of articles:", df.count())
    
    # Articles by source
    source_counts = df.groupBy("source") \
        .count() \
        .orderBy(desc("count"))
    
    # Average sentiment by source
    sentiment_by_source = df.groupBy("source") \
        .agg(avg("sentiment").alias("avg_sentiment")) \
        .orderBy(desc("avg_sentiment"))
    
    return source_counts, sentiment_by_source

def save_to_delta(df, table_name):
    """
    Save analysis results to Delta Lake
    """
    delta_path = f"{STORAGE_CONFIG['delta_lake_path']}/{table_name}"
    
    df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(delta_path)

def main():
    # Extract news data
    articles = extract_news_data()
    if not articles:
        print("No articles to analyze")
        return
    
    # Process into DataFrame
    news_df = process_news_data(articles)
    
    # Perform analysis
    source_counts, sentiment_by_source = analyze_news(news_df)
    
    # Save results
    save_to_delta(source_counts, "news_source_counts")
    save_to_delta(sentiment_by_source, "news_sentiment_analysis")
    
    # Display results
    print("\nTop 5 news sources by article count:")
    source_counts.show(5)
    
    print("\nTop 5 news sources by average sentiment:")
    sentiment_by_source.show(5)

if __name__ == "__main__":
    main() 