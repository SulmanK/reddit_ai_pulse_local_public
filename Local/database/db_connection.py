"""
Database Connection Module

This module handles PostgreSQL database connections for the Reddit Text Insight & Sentiment project.
It provides functions for establishing and closing database connections.

Owner: Sulman Khan
"""

import psycopg2
from config.config import DB_PARAMS
import logging

def get_db_connection():
    """
    Establishes a connection to the PostgreSQL database.

    Returns:
        psycopg2.extensions.connection: The database connection object.

    Raises:
        Exception: If there is an error connecting to the database.
    """
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        logging.info("Connected to the database.")
        return conn
    except Exception as e:
        logging.error(f"Error connecting to the database: {e}")
        raise

def close_db_connection(conn):
    """
    Closes the database connection.

    Args:
        conn (psycopg2.extensions.connection): The database connection object.
    """
    if conn:
        conn.close()
        logging.info("Database connection closed.")
