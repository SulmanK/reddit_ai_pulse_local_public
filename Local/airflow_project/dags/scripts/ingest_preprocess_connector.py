"""
Data Ingestion and Preprocessing Connector

This script connects the Airflow DAG to the Reddit data ingestion and preprocessing pipeline.
It handles the initial data collection from Reddit API and preprocessing steps.

Owner: Sulman Khan
"""

import os
import sys
import logging
from pathlib import Path

# Setup paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DAGS_DIR = os.path.dirname(os.path.dirname(SCRIPT_DIR))
AIRFLOW_DIR = os.path.dirname(DAGS_DIR)
PROJECT_ROOT = os.path.dirname(AIRFLOW_DIR)
LOCAL_SCRIPTS_DIR = os.path.join(PROJECT_ROOT, 'Local', 'scripts')

# Add necessary paths
sys.path.extend([PROJECT_ROOT, LOCAL_SCRIPTS_DIR])

def ingest_preprocess_process():
    """
    Connector function to run the ingest and preprocess pipeline.
    
    Imports and executes the main ingestion and preprocessing module,
    with comprehensive logging for debugging path and import issues.
    
    Raises:
        ImportError: If the required modules cannot be imported
        Exception: For any other errors during execution
    """
    try:
        from Local.scripts.ingest_preprocess import ingest_and_preprocess
        
        logging.info(f"Current working directory: {os.getcwd()}")
        logging.info(f"Project root: {PROJECT_ROOT}")
        logging.info(f"Scripts directory: {LOCAL_SCRIPTS_DIR}")
        logging.info(f"Python path: {sys.path}")
        
        ingest_and_preprocess()
    except ImportError as e:
        logging.error(f"Import error: {e}")
        logging.error(f"Current working directory: {os.getcwd()}")
        logging.error(f"Project root: {PROJECT_ROOT}")
        logging.error(f"Scripts directory: {LOCAL_SCRIPTS_DIR}")
        logging.error(f"Python path: {sys.path}")
        raise
    except Exception as e:
        logging.error(f"Error running ingest and preprocess: {e}")
        raise

if __name__ == "__main__":
    ingest_preprocess_process()