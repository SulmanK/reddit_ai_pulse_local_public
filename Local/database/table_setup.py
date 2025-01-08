"""
Database Table Setup Module

This module handles the creation of all necessary database schemas and tables
for the Reddit Text Insight & Sentiment project.

Owner: Sulman Khan
"""

import psycopg2
import logging
from config.config import DB_PARAMS
from utils.custom_logging import setup_logging, get_logger
from psycopg2 import sql
import json
from datetime import datetime

# Set up module-level logger
logger = get_logger(__name__)  # This will create a logger named 'reddit_pipeline.database.table_setup'

def create_subreddit_table(conn: psycopg2.extensions.connection, subreddit_name: str) -> None:
    """
    Creates a new table for a given subreddit in the database.
    """
    table_name = f"raw_{subreddit_name.lower()}"
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS raw_data;")
            
            cur.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS raw_data.{table_name} (
                    post_id TEXT PRIMARY KEY,
                    subreddit TEXT NOT NULL,
                    title TEXT,
                    author TEXT,
                    url TEXT,
                    score INT,
                    created_utc TIMESTAMP,
                    comments JSONB
                );
            """).format(table_name=sql.Identifier(table_name)))
            
            conn.commit()
            logger.info(f"Table 'raw_data.{table_name}' created successfully.")
    except Exception as e:
        logger.error(f"Error creating table for subreddit '{subreddit_name}': {e}", exc_info=True)
        conn.rollback()
        raise

def insert_raw_post_data(conn, post_data):
    """
    Inserts post data into the database table.

    Args:
        conn (psycopg2.extensions.connection): The database connection object.
        post_data (dict): Dictionary containing post data.
    
    Raises:
        Exception: If there is an error inserting the data.
    """
    table_name = f"raw_{post_data['subreddit'].lower()}"
    create_subreddit_table(conn, post_data['subreddit'])
    
    try:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("""
                INSERT INTO raw_data.{table_name} (
                    subreddit, post_id, title, author, url, score, created_utc, comments
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (post_id) DO NOTHING;
            """).format(table_name=sql.Identifier(table_name)), (
                post_data["subreddit"],
                post_data["post_id"],
                post_data["title"],
                post_data["author"],
                post_data["url"],
                post_data["score"],
                datetime.utcfromtimestamp(post_data["created_utc"]),
                json.dumps(post_data["comments"]),
            ))
        conn.commit()
        logger.info(f"Inserted post {post_data['post_id']} into raw_data.{table_name}")
    except Exception as e:
        logger.error(f"Error inserting post data: {e}")
        conn.rollback()
        raise




def create_schema_and_base_tables(conn):
    """
    Creates the 'processed_data' schema if it doesn't exist.

    Args:
        conn: Database connection object.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS processed_data;")
            conn.commit()
            logger.info("Schema 'processed_data' created successfully.")
    except Exception as e:
        logger.error(f"Schema creation error: {e}")
        raise

def create_tables_per_subreddit(conn, subreddit):
    """
    Creates posts and comments tables for a specific subreddit.

    Args:
        conn: Database connection object.
        subreddit (str): The subreddit name.
    """
    try:
        with conn.cursor() as cur:
            # Create posts table with post_id as primary key
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS processed_data.posts_{subreddit.lower()} (
                    post_id TEXT PRIMARY KEY,
                    subreddit TEXT,
                    title TEXT,
                    author TEXT,
                    url TEXT,
                    score INTEGER,
                    created_utc TIMESTAMPTZ
                    
                );
            """)
            
            # Create comments table with comment_id as primary key
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS processed_data.comments_{subreddit.lower()} (
                    comment_id TEXT PRIMARY KEY,
                    post_id TEXT,
                    body TEXT,
                    author TEXT,
                    created_utc TIMESTAMPTZ
                    
                );
            """)
            conn.commit()
            logger.info(f"Tables created for {subreddit}.")
    except Exception as e:
        logger.error(f"Table creation error for {subreddit}: {e}")
        raise

def create_daily_summary_table(conn):
    """
    Creates the daily summary table if it doesn't already exist in the processed_data schema.

    Args:
        conn: Database connection object.
    """
    create_daily_summary_table_query = """
    CREATE TABLE IF NOT EXISTS processed_data.daily_summary_data (
        id SERIAL,
        subreddit TEXT,
        post_id TEXT,
        post_score INTEGER,
        post_url TEXT,
        comment_id TEXT,
        summary_date DATE,
        processed_date TIMESTAMPTZ,
        needs_processing BOOLEAN DEFAULT TRUE,
        post_content TEXT,
        comment_body TEXT,
        PRIMARY KEY (post_id, comment_id)
    );
    """

    try:
        with conn.cursor() as cur:
            cur.execute(create_daily_summary_table_query)
            conn.commit()
            logger.info("Daily summary table created successfully (if it did not exist).")
    except Exception as e:
        logger.error(f"An error occurred while creating the daily summary table: {e}")
        raise

def create_text_summary_table(conn):
    """
    Creates the text summary table if it doesn't already exist in the processed_data schema.

    Args:
        conn: Database connection object.
    """
    create_text_summary_table_query = """
    CREATE TABLE IF NOT EXISTS processed_data.text_summary_results (
        comment_id TEXT PRIMARY KEY,
        comment_summary TEXT
        
    );
    """
    
    try:
        with conn.cursor() as cur:
            cur.execute(create_text_summary_table_query)
            conn.commit()
            logger.info("Text summary table created successfully (if it did not exist).")
    except Exception as e:
        logger.error(f"An error occurred while creating the text summary table: {e}")
        raise

def create_sentiment_analysis_table(conn):
    """
    Creates the sentiment analysis table if it doesn't already exist in the processed_data schema.

    Args:
        conn: Database connection object.
    """
    create_sentiment_analysis_table_query = """
    CREATE TABLE IF NOT EXISTS processed_data.sentiment_analysis_results (
        comment_id TEXT PRIMARY KEY,
        sentiment_score FLOAT,
        sentiment_label TEXT
    );
    """

    try:
        with conn.cursor() as cur:
            cur.execute(create_sentiment_analysis_table_query)
            conn.commit()
            logger.info("Sentiment analysis table created successfully (if it did not exist).")
    except Exception as e:
        logger.error(f"An error occurred while creating the sentiment analysis table: {e}")
        raise

def create_processing_metadata_table(conn):
    """
    Creates a table to track processing metadata like last processed timestamps.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS processed_data.processing_metadata (
                    subreddit TEXT PRIMARY KEY,
                    last_processed_utc BIGINT,
                    last_run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            conn.commit()
            logger.info("Processing metadata table created successfully.")
    except Exception as e:
        logger.error(f"Error creating processing metadata table: {e}")
        raise

if __name__ == '__main__':
    # Set up logging before any other operations
    logger = setup_logging(log_dir='logs')
    logger.info("Starting database table setup...")
    
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        create_schema_and_base_tables(conn)
        create_text_summary_table(conn)
        create_sentiment_analysis_table(conn)
        create_processing_metadata_table(conn)
        logger.info("Setup completed successfully")
    except Exception as e:
        logger.error(f"Setup error: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()