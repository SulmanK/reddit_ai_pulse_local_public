"""
Sentiment Analysis Module

This module performs emotion analysis on Reddit comments using a pre-trained model.
It processes comments from the database and saves the sentiment analysis results.

Owner: Sulman Khan
"""

import os
import sys
import logging
from datetime import datetime
import psycopg2
from transformers import pipeline, AutoModelForSequenceClassification, AutoTokenizer
import mlflow
import mlflow.transformers

# Add the Local directory to Python path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOCAL_DIR = os.path.dirname(SCRIPT_DIR)
sys.path.append(LOCAL_DIR)

from config.config import get_database_config
from utils.custom_logging import setup_logging

# Define emotion labels mapping
EMOTION_LABELS = {
    'admiration': 'Admiration', 'amusement': 'Amusement', 'anger': 'Anger',
    'annoyance': 'Annoyance', 'approval': 'Approval', 'caring': 'Caring',
    'confusion': 'Confusion', 'curiosity': 'Curiosity', 'desire': 'Desire',
    'disappointment': 'Disappointment', 'disapproval': 'Disapproval',
    'disgust': 'Disgust', 'embarrassment': 'Embarrassment',
    'excitement': 'Excitement', 'fear': 'Fear', 'gratitude': 'Gratitude',
    'grief': 'Grief', 'joy': 'Joy', 'love': 'Love',
    'nervousness': 'Nervousness', 'optimism': 'Optimism', 'pride': 'Pride',
    'realization': 'Realization', 'relief': 'Relief', 'remorse': 'Remorse',
    'sadness': 'Sadness', 'surprise': 'Surprise', 'neutral': 'Neutral'
}

def initialize_emotion_analyzer():
    """
    Initialize the emotion analysis model and tokenizer.
    
    Returns:
        pipeline: The emotion analysis pipeline
        
    Raises:
        Exception: If there is an error loading the model
    """
    try:
        model_name = "SamLowe/roberta-base-go_emotions"
        local_model_path = "/models/models--SamLowe--roberta-base-go_emotions/snapshots/58b6c5b44a7a12093f782442969019c7e2982299"

        model = AutoModelForSequenceClassification.from_pretrained(local_model_path)
        tokenizer = AutoTokenizer.from_pretrained(local_model_path)

        emotion_analyzer = pipeline(
            "text-classification",
            model=model,
            tokenizer=tokenizer
        )

        return emotion_analyzer
    except Exception as e:
        logging.error(f"Error initializing emotion analyzer: {e}")
        raise

def truncate_text(text, max_length=500):
    """
    Truncates text to a maximum length while trying to preserve complete sentences.
    
    Args:
        text (str): The text to truncate
        max_length (int): Maximum length of text (default 500 to stay well within model limits)
    
    Returns:
        str: Truncated text
    """
    if not text or len(text) <= max_length:
        return text
    
    # Try to find the last sentence boundary before max_length
    truncated = text[:max_length]
    last_period = truncated.rfind('.')
    last_question = truncated.rfind('?')
    last_exclamation = truncated.rfind('!')
    
    # Find the last sentence boundary
    cut_point = max(last_period, last_question, last_exclamation)
    
    # If no sentence boundary found, just cut at max_length
    if cut_point == -1:
        return truncated.rsplit(' ', 1)[0] + '...'
    
    return text[:cut_point + 1]

def perform_sentiment_analysis(text, emotion_analyzer):
    """
    Perform sentiment analysis on the given text.
    
    Args:
        text (str): Text to analyze
        emotion_analyzer: The emotion analysis model
    
    Returns:
        tuple: (sentiment_label, sentiment_score, emotion_description)
    """
    try:
        if not text or text.strip() == "":
            return "Neutral", 0.0, "No content"
            
        # Truncate text before analysis
        truncated_text = truncate_text(text)
        
        # Perform emotion analysis
        emotion_output = emotion_analyzer(truncated_text)
        
        if not emotion_output or len(emotion_output) == 0:
            return "Neutral", 0.0, "Analysis failed"
            
        # Get the predicted emotion and score
        emotion_label = emotion_output[0]['label']
        emotion_score = emotion_output[0]['score']
        
        # Map emotion to sentiment (you can customize this mapping)
        sentiment_mapping = {
            'joy': 'Positive',
            'love': 'Positive',
            'admiration': 'Positive',
            'approval': 'Positive',
            'caring': 'Positive',
            'excitement': 'Positive',
            'gratitude': 'Positive',
            'pride': 'Positive',
            'optimism': 'Positive',
            'relief': 'Positive',
            'anger': 'Negative',
            'annoyance': 'Negative',
            'disappointment': 'Negative',
            'disapproval': 'Negative',
            'disgust': 'Negative',
            'embarrassment': 'Negative',
            'fear': 'Negative',
            'grief': 'Negative',
            'nervousness': 'Negative',
            'remorse': 'Negative',
            'sadness': 'Negative',
            'confusion': 'Neutral',
            'curiosity': 'Neutral',
            'realization': 'Neutral',
            'surprise': 'Neutral',
            'neutral': 'Neutral'
        }
        
        sentiment_label = sentiment_mapping.get(emotion_label.lower(), 'Neutral')
        
        return sentiment_label, float(emotion_score), emotion_label
        
    except Exception as e:
        logging.error(f"Error during sentiment analysis: {str(e)}")
        return "Neutral", 0.0, "Error in analysis"

def analyze_sentiment():
    """
    Main function to perform sentiment analysis on Reddit comments.
    
    Pipeline steps:
    1. Set up logging and MLflow tracking
    2. Initialize sentiment analyzer
    3. Connect to database
    4. Process comments and save results
    
    Note:
        - Uses MLflow for experiment tracking
        - Implements batch processing
        - Handles database transactions safely
    """
    setup_logging()
    conn = None
    cur = None

    try:
        # Update MLflow setup to use server URL instead of filesystem
        mlflow.set_tracking_uri("http://mlflow:5000")
        mlflow.set_experiment("reddit_sentiment_analysis_experiments")
        
        with mlflow.start_run() as run:
            mlflow.log_param("model_name", "SamLowe/roberta-base-go_emotions")
            
            # Initialize components
            emotion_analyzer = initialize_emotion_analyzer()
            logging.info("Emotion analysis model loaded")

            # Database connection
            db_config = get_database_config()
            conn = psycopg2.connect(**db_config)
            cur = conn.cursor()
            
            # Process data
            cur.execute("""
                SELECT 
                    post_id, subreddit, post_score, post_url, comment_id,
                    summary_date, post_content, comment_body
                FROM processed_data.current_summary_staging
            """)
            rows = cur.fetchall()
            logging.info(f"Fetched {len(rows)} rows from database")

            # Process each comment
            for i, row in enumerate(rows):
                try:
                    # Extract only needed data
                    comment_id = row[4]  # Assuming comment_id is still at index 4
                    comment_body = row[7]  # Assuming comment_body is still at index 7
                    
                    # Perform sentiment analysis
                    sentiment_label, sentiment_score, emotion_description = perform_sentiment_analysis(
                        comment_body, emotion_analyzer
                    )
                    
                    # Save results with simplified schema
                    cur.execute("""
                        INSERT INTO processed_data.sentiment_analysis_results (
                            comment_id,
                            sentiment_score,
                            sentiment_label
                        ) VALUES (%s, %s, %s)
                        ON CONFLICT (comment_id) DO UPDATE SET
                            sentiment_score = EXCLUDED.sentiment_score,
                            sentiment_label = EXCLUDED.sentiment_label
                    """, (comment_id, sentiment_score, sentiment_label))
                    
                    conn.commit()
                    logging.info(f"Sentiment analysis added for comment_id: {comment_id}")
                    
                except Exception as e:
                    logging.error(f"Error processing comment {comment_id}: {str(e)}")
                    continue

    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
            logging.info("Database connection closed")

if __name__ == "__main__":
    analyze_sentiment()