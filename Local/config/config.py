"""
Configuration Management Module

This module manages configuration settings for the Reddit Text Insight & Sentiment project.
It handles database connections, API credentials, and environment-specific settings.

Owner: Sulman Khan
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Define Docker and Local database configurations
DB_PARAMS = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"), 
    "port": int(os.getenv("DB_PORT"))
}


# Reddit API configuration
REDDIT_PARAMS = {
    "client_id": os.getenv("REDDIT_CLIENT_ID"),
    "client_secret": os.getenv("REDDIT_CLIENT_SECRET"),
    "username": os.getenv("REDDIT_USERNAME"),
    "password": os.getenv("REDDIT_PASSWORD"),
    "user_agent": os.getenv("REDDIT_USER_AGENT")
}

# Target subreddits for data collection
SUBREDDITS = ["dataengineering", "machinelearning", "datascience", "claudeai",
               "singularity", "localllama", "openai", "stablediffusion"]

# Google Gemini API configuration
GOOGLE_GEMINI_API_KEY = os.getenv("GOOGLE_GEMINI_API_KEY")

def get_database_config():
    """
    Retrieves the current database configuration.
    
    Returns:
        dict: Database connection parameters based on the current environment
    """
    return DB_PARAMS