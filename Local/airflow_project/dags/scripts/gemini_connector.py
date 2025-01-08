"""
Gemini Analysis Connector

This script connects the Airflow DAG to the Gemini LLM analysis process.
It imports and executes the main Gemini analyzer module from the Local/scripts directory.

Owner: Sulman Khan
"""

import os
import sys
import logging
from pathlib import Path

# Setup paths
PROJECT_ROOT = os.getenv('PROJECT_ROOT', os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../..')))
sys.path.append(PROJECT_ROOT)

def gemini_analysis_process():
    """
    Connector function to run the Gemini analysis process.
    
    Imports and executes the Gemini analyzer module, with error handling
    and logging for import and execution failures.
    
    Raises:
        ImportError: If the Gemini analyzer module cannot be imported
        Exception: For any other errors during execution
    """
    try:
        from Local.scripts.gemini_analyzer import analyze_data
        analyze_data()
    except ImportError as e:
        logging.error(f"Import error: {e}")
        logging.error(f"PROJECT_ROOT: {PROJECT_ROOT}")
        logging.error(f"Python path: {sys.path}")
        raise
    except Exception as e:
        logging.error(f"Error running Gemini analysis: {e}")
        raise

if __name__ == "__main__":
    gemini_analysis_process()