"""
Spark Session Management Module

This module provides utility functions for creating and managing Spark sessions
for the Reddit Text Insight & Sentiment project.

Owner: Sulman Khan
"""

from pyspark.sql import SparkSession
import logging
import os

def create_spark_session():
    """
    Creates a SparkSession with necessary configurations.
    Includes PostgreSQL JDBC driver for database connectivity.

    Returns:
        SparkSession: A configured SparkSession object.

    Raises:
        Exception: If there is an error creating the Spark session.
    """
    try:
        spark = SparkSession.builder \
                .appName("RedditDataPreprocessing") \
                .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4") \
                .getOrCreate()
        logging.info("Spark session created successfully.")
        return spark
    except Exception as e:
        logging.error(f"An error occurred while creating Spark Session: {e}")
        raise

def stop_spark_session(spark):
    """
    Safely stops the provided Spark session.

    Args:
        spark (SparkSession): The Spark session to stop.
    """
    if spark.sparkContext:
        spark.stop()
        logging.info("Spark session stopped successfully.")