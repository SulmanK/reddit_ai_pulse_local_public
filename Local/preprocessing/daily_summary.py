"""
Daily Summary Generation Module

This module handles the generation of daily summaries from processed Reddit data using PySpark.
It aggregates posts and comments data for each subreddit on a daily basis.

Owner: Sulman Khan
"""

import pyspark.sql.functions as F
from datetime import datetime
from utils.custom_logging import get_logger
from config.config import DB_PARAMS, SUBREDDITS

# Configure logging
logger = get_logger(__name__)  # This will create a logger named 'reddit_pipeline.preprocessing.daily_summary'

jdbc_url = f"jdbc:postgresql://{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['dbname']}"


def get_last_processed_timestamp(spark):
    """
    Gets the last processed timestamp from the daily_summary table.
    """
    try:
        summary_df = spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "processed_data.daily_summary_data") \
            .option("user", DB_PARAMS["user"]) \
            .option("password", DB_PARAMS["password"]) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        
        last_processed = summary_df.agg(F.max("processed_date")).collect()[0][0]
        return last_processed if last_processed else None
    except Exception as e:
        logger.warning(f"No existing summary data found: {e}")
        return None

def generate_daily_summaries(spark):
    """
    Generates daily summaries of the processed data using PySpark.
    Implements incremental loading by only processing data since the last processed timestamp.
    """
    current_timestamp = datetime.now()
    last_processed = get_last_processed_timestamp(spark)
    total_summaries_added = 0
    
    if last_processed:
        logger.info(f"Processing summaries from {last_processed} to {current_timestamp}")
    else:
        logger.info("No previous summaries found. Processing all available data.")

    for subreddit in SUBREDDITS:
        logger.info(f"Processing daily summary for subreddit: {subreddit}")

        # Load posts and comments tables
        posts_df = spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", f"processed_data.posts_{subreddit.lower()}") \
            .option("user", DB_PARAMS["user"]) \
            .option("password", DB_PARAMS["password"]) \
            .option("driver", "org.postgresql.Driver") \
            .load()

        posts_count = posts_df.count()
        logger.info(f"Found {posts_count} posts for {subreddit}")

        comments_df = spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", f"processed_data.comments_{subreddit.lower()}") \
            .option("user", DB_PARAMS["user"]) \
            .option("password", DB_PARAMS["password"]) \
            .option("driver", "org.postgresql.Driver") \
            .load()

        comments_count = comments_df.count()
        logger.info(f"Found {comments_count} comments for {subreddit}")

        # Filter posts and comments based on created_utc timestamp
        if last_processed:
            daily_posts_df = posts_df.filter(
                (F.col("created_utc") > last_processed) &
                (F.col("created_utc") <= current_timestamp)
            )

            daily_comments_df = comments_df.filter(
                (F.col("created_utc") > last_processed) &
                (F.col("created_utc") <= current_timestamp)
            )
        else:
            daily_posts_df = posts_df.filter(F.col("created_utc") <= current_timestamp)
            daily_comments_df = comments_df.filter(F.col("created_utc") <= current_timestamp)

        filtered_posts_count = daily_posts_df.count()
        logger.info(f"Filtered to {filtered_posts_count} posts for processing")

        if filtered_posts_count == 0:
            logger.info(f"No new posts to summarize for {subreddit}")
            continue

        # Join posts and comments on post_id
        joined_df = daily_posts_df.alias("posts").join(
            daily_comments_df.alias("comments"), 
            "post_id", 
            "right"
        )
        
        # Prepare the daily summary
        daily_summary_df = joined_df.select(
            F.col("subreddit"),
            F.col("posts.post_id").alias("post_id"),
            F.col("posts.score").alias("post_score"),
            F.col("posts.url").alias("post_url"),
            F.col("comments.comment_id").alias("comment_id"),
            F.to_date(F.col("comments.created_utc")).alias("summary_date"),
            F.current_timestamp().alias("processed_date"),
            F.col("posts.title").alias("post_content"),
            F.col("comments.body").alias("comment_body")
        )

        # Filter out rows where required fields are null
        daily_summary_df = daily_summary_df.filter(
            (F.col("comment_body").isNotNull()) & 
            (F.col("comment_id").isNotNull()) &
            (F.col("post_id").isNotNull())
        )

        filtered_summaries_count = daily_summary_df.count()
        logger.info(f"Generated {filtered_summaries_count} summaries before deduplication")

        # Deduplication Logic
        existing_summary_df = spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "processed_data.daily_summary_data") \
            .option("user", DB_PARAMS["user"]) \
            .option("password", DB_PARAMS["password"]) \
            .option("driver", "org.postgresql.Driver") \
            .load()

        # Perform a left anti-join to get only new records
        new_daily_summary_df = daily_summary_df.join(
            existing_summary_df,
            ["post_id", "comment_id"],  # Using composite key for deduplication
            "left_anti"
        )

        new_summaries_count = new_daily_summary_df.count()
        
        if new_summaries_count > 0:
            # Write the new daily summaries to PostgreSQL
            new_daily_summary_df.write.format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", "processed_data.daily_summary_data") \
                .option("user", DB_PARAMS["user"]) \
                .option("password", DB_PARAMS["password"]) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            
            total_summaries_added += new_summaries_count
            logger.info(f"Successfully added {new_summaries_count} new summaries for {subreddit}")
        else:
            logger.info(f"No new unique summaries to add for {subreddit}")

    logger.info(f"Processing complete. Total new summaries added: {total_summaries_added}")