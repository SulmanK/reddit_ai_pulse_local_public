"""
Text Summarization Module

This module handles the summarization of Reddit comments using a pre-trained BART model.
It processes comments from the database and saves the generated summaries.

Owner: Sulman Khan
"""

import os
import sys
from datetime import datetime
import psycopg2
from transformers import pipeline, AutoTokenizer, AutoModelForSeq2SeqLM
import mlflow
import mlflow.transformers
import torch

# Add the Local directory to Python path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOCAL_DIR = os.path.dirname(SCRIPT_DIR)
sys.path.append(LOCAL_DIR)

from config.config import get_database_config
from utils.custom_logging import get_logger

# Initialize logger
logger = get_logger(__name__)  # This will create a logger named 'reddit_pipeline.scripts.summarize'

def create_summarizer():
    """
    Initialize and return the summarization model.
    
    Returns:
        pipeline: The summarization pipeline
        
    Raises:
        Exception: If there is an error loading the model
    """
    try:
        # Set environment variable for tokenizer
        os.environ["TOKENIZERS_PARALLELISM"] = "false"
        
        # Load model and tokenizer
        model_name = "philschmid/bart-large-cnn-samsum"
        local_model_path = "/models/models--philschmid--bart-large-cnn-samsum/snapshots/e49b3d60d923f12db22bdd363356f1a4c68532ad"

        tokenizer = AutoTokenizer.from_pretrained(local_model_path)
        model = AutoModelForSeq2SeqLM.from_pretrained(
            local_model_path,
            torch_dtype=torch.float32,
            device_map="auto",
            low_cpu_mem_usage=True
        )
        
        summarizer = pipeline(
            "summarization",
            model=model,
            tokenizer=tokenizer,
            framework="pt"
        )
        
        return summarizer
    except Exception as e:
        logger.error(f"Error loading summarization model: {str(e)}")
        raise

def generate_summary(comment_body, summarizer):
    """
    Generate summary for a given comment.
    
    Args:
        comment_body (str): Text to summarize
        summarizer: The summarization pipeline
        
    Returns:
        str: Summarized text or original text if short enough
    """
    if not comment_body:
        return ""
        
    if len(comment_body.split()) <= 59:
        logger.info("Comment is less than the max length")
        return comment_body 
        
    try:
        summary = summarizer(comment_body, max_length=60, min_length=15, do_sample=False, truncation=True)
        if summary and isinstance(summary, list) and len(summary) > 0:
            return summary[0]['summary_text']
    except Exception as e:
        logger.error(f"Summarization failed: {e}")
    
    return ""

def summarize_posts():
    """
    Main function to summarize Reddit posts and comments.
    
    Pipeline steps:
    1. Set up logging and MLflow tracking
    2. Initialize summarizer model
    3. Connect to database
    4. Process comments and save summaries
    
    Note:
        - Uses MLflow for experiment tracking
        - Implements efficient text processing
        - Handles database transactions safely
    """
    conn = None
    cur = None

    try:
        # MLflow setup
        mlflow.set_tracking_uri("http://mlflow:5000")
        mlflow.set_experiment("reddit_summarization_experiments")
        
        with mlflow.start_run() as run:
            # Log parameters
            mlflow.log_param("model_name", "philschmid/bart-large-cnn-samsum")
            mlflow.log_param("max_length", 60)
            mlflow.log_param("min_length", 15)
            mlflow.log_param("do_sample", False)

            # Initialize components
            summarizer = create_summarizer()
            logger.info("Summarization model loaded")

            # Database connection
            db_config = get_database_config()
            conn = psycopg2.connect(**db_config)
            cur = conn.cursor()

            # Fetch data
            cur.execute("""
                SELECT 
                    post_id, subreddit, post_score, post_url, comment_id,
                    summary_date, post_content, comment_body
                FROM processed_data.current_summary_staging
            """)
            rows = cur.fetchall()
            logger.info(f"Fetched {len(rows)} rows from database")

            # Process each comment
            for i, row in enumerate(rows):
                try:
                    # Extract only needed data
                    comment_id = row[4]  
                    comment_body = row[7] 
                    
                    # Generate summary
                    summary_text = generate_summary(comment_body, summarizer)
                    
                    # Save to database with simplified schema
                    cur.execute("""
                        INSERT INTO processed_data.text_summary_results (
                            comment_id,
                            comment_summary
                        ) VALUES (%s, %s)
                        ON CONFLICT (comment_id) DO UPDATE SET
                            comment_summary = EXCLUDED.comment_summary
                    """, (comment_id, summary_text))
                    
                    conn.commit()
                    logger.info(f"Summary added for comment_id: {comment_id}")
                    
                except Exception as e:
                    logger.error(f"Error processing comment {comment_id}: {str(e)}")
                    continue

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    summarize_posts()