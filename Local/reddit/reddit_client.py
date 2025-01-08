"""
Reddit API Client Module

This module handles the interaction with Reddit's API using PRAW.
It manages data collection from specified subreddits and prepares it for storage.

Owner: Sulman Khan
"""

import praw
import logging
from config.config import REDDIT_PARAMS

def create_reddit_instance():
    """
    Creates an instance of the Reddit client (praw.Reddit).

    Returns:
        praw.Reddit: An instance of the Reddit API client.

    Raises:
        Exception: If there is an error connecting to the Reddit API.
    """
    try:
        reddit = praw.Reddit(**REDDIT_PARAMS)
        logging.info("Connected to Reddit API.")
        return reddit
    except Exception as e:
        logging.error(f"Error connecting to Reddit API: {e}")
        raise

def get_last_processed_timestamp(conn, subreddit_name):
    """
    Gets the last processed timestamp for a subreddit.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT last_processed_utc 
                FROM processed_data.processing_metadata 
                WHERE subreddit = %s
            """, (subreddit_name,))
            result = cur.fetchone()
            return result[0] if result else 0
    except Exception as e:
        logging.error(f"Error getting last processed timestamp: {e}")
        return 0

def update_last_processed_timestamp(conn, subreddit_name, timestamp):
    """
    Updates the last processed timestamp for a subreddit.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO processed_data.processing_metadata (subreddit, last_processed_utc)
                VALUES (%s, %s)
                ON CONFLICT (subreddit) 
                DO UPDATE SET 
                    last_processed_utc = EXCLUDED.last_processed_utc,
                    last_run_timestamp = CURRENT_TIMESTAMP
            """, (subreddit_name, timestamp))
            conn.commit()
    except Exception as e:
        logging.error(f"Error updating last processed timestamp: {e}")
        conn.rollback()
        raise

def fetch_and_save_posts(reddit, subreddit_name, table_setup, conn):
    """
    Fetches new posts from a subreddit since the last processed timestamp.
    """
    try:
        last_processed_utc = get_last_processed_timestamp(conn, subreddit_name)
        current_max_utc = 0
        
        subreddit = reddit.subreddit(subreddit_name)
        for submission in subreddit.hot(limit=20):
            # Skip already processed posts
            if submission.created_utc <= last_processed_utc:
                continue
                
            current_max_utc = max(current_max_utc, submission.created_utc)
            
            post_data = {
                "subreddit": subreddit_name,
                "post_id": submission.id,
                "title": str(submission.title),
                "author": str(submission.author),
                "url": str(submission.url),
                "score": int(submission.score),
                "created_utc": submission.created_utc,
                "comments": []
            }
            
            if submission.num_comments > 0:
                try:
                    submission.comments.replace_more(limit=0)
                    post_data["comments"] = [
                        {
                            "comment_id": comment.id,
                            "author": str(comment.author),
                            "body": str(comment.body),
                            "created_utc": comment.created_utc,
                        }
                        for comment in submission.comments.list()[:10]
                        if comment.created_utc > last_processed_utc
                    ]
                except Exception as e:
                    logging.error(f"Error fetching comments for post {submission.id}: {e}")

            table_setup.insert_raw_post_data(conn, post_data)
            logging.info(f"Successfully processed post {submission.id} from r/{subreddit_name}")
            
        if current_max_utc > last_processed_utc:
            update_last_processed_timestamp(conn, subreddit_name, current_max_utc)
            
    except Exception as e:
        logging.error(f"Error processing subreddit {subreddit_name}: {e}")
        raise

if __name__ == "__main__":
    reddit = create_reddit_instance()
    # Add test code here if needed