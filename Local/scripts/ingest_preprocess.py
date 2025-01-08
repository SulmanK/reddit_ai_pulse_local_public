"""
Data Ingestion and Preprocessing Module

This module orchestrates the complete data pipeline for Reddit data collection,
from ingestion through preprocessing. It coordinates multiple components to
fetch, store, and process Reddit data.

Owner: Sulman Khan
"""

import os
import sys
import logging

# Add the Local directory to Python path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOCAL_DIR = os.path.dirname(SCRIPT_DIR)
sys.path.append(LOCAL_DIR)

from utils.custom_logging import setup_logging
from database import db_connection, table_setup
from reddit import reddit_client
from config.config import SUBREDDITS
from preprocessing import spark_utils, data_processing, daily_summary


def ingest_and_preprocess():
    """
    Main function to run the Reddit data ingestion and preprocessing pipeline.
    
    Pipeline steps:
    1. Fetch data from Reddit API and save to database
    2. Set up necessary database tables and schemas
    3. Preprocess the raw data
    4. Generate daily summaries using PySpark
    
    Note:
        - Ensures proper connection handling
        - Implements comprehensive logging
        - Handles errors at each pipeline stage
    """
    setup_logging()
    spark = spark_utils.create_spark_session()
    conn = None

    try:
        # Establish connections
        conn = db_connection.get_db_connection()
        reddit = reddit_client.create_reddit_instance()
        
        table_setup.create_schema_and_base_tables(conn)
        table_setup.create_processing_metadata_table(conn)  # Add this line
        # 1. Fetch and save Reddit data
        for subreddit_name in SUBREDDITS:
            logging.info(f"Fetching data for subreddit: {subreddit_name}")
            reddit_client.fetch_and_save_posts(reddit, subreddit_name, table_setup, conn)

        # 2. Set up database tables
        table_setup.create_daily_summary_table(conn)
        table_setup.create_text_summary_table(conn)
        table_setup.create_sentiment_analysis_table(conn)
        
        for subreddit in SUBREDDITS:
            table_setup.create_tables_per_subreddit(conn, subreddit)

        # 3. Preprocess raw data
        data_processing.preprocess_data(spark, conn)

        # 4. Generate daily summaries
        daily_summary.generate_daily_summaries(spark)

    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        raise
    finally:
        spark_utils.stop_spark_session(spark)
        if conn:
            db_connection.close_db_connection(conn)

if __name__ == "__main__":
    ingest_and_preprocess()