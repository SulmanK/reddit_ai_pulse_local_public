"""
Text Summarization Connector

This script connects the Airflow DAG to the text summarization process.
It handles the generation of summaries for Reddit posts and comments.

Owner: Sulman Khan
"""

import os
import sys
import logging

# Add the Local directory to Python path
AIRFLOW_HOME = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
sys.path.append(AIRFLOW_HOME)

def summarize_process():
    """
    Function to run the summarization process.
    
    Imports and executes the text summarization module for
    generating summaries of Reddit content using BART model.
    
    Raises:
        Exception: For any errors during execution
    """
    try:
        from Local.scripts.summarize import summarize_posts
        summarize_posts()
    except Exception as e:
        logging.error(f"Error in summarization process: {e}")
        raise e

if __name__ == "__main__":
    summarize_process()