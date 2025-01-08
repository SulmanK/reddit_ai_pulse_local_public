"""
Sentiment Analysis Connector

This script connects the Airflow DAG to the sentiment analysis process.
It handles the execution of sentiment analysis on processed Reddit data.

Owner: Sulman Khan
"""

import os
import sys
import logging

# Add the Local directory to Python path
AIRFLOW_HOME = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
sys.path.append(AIRFLOW_HOME)

def sentiment_analysis_process():
    """
    Function to run the sentiment analysis process.
    
    Imports and executes the sentiment analysis module for
    analyzing Reddit post and comment sentiments.
    
    Raises:
        Exception: For any errors during execution
    """
    try:
        from Local.scripts.sentiment_analysis import analyze_sentiment
        analyze_sentiment()
    except Exception as e:
        logging.error(f"Error in sentiment analysis: {e}")
        raise e

if __name__ == "__main__":
    sentiment_analysis_process()