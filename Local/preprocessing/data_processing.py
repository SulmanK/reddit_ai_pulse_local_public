"""
Data Processing Module

This module handles the preprocessing of raw Reddit data using PySpark.
It includes functions for cleaning and transforming posts and comments data.

Owner: Sulman Khan
"""

import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType, DoubleType, BooleanType
from utils.custom_logging import get_logger
from config.config import DB_PARAMS, SUBREDDITS
from better_profanity import Profanity
import re

# Configure logging
logger = get_logger(__name__)  # This will create a logger named 'reddit_pipeline.preprocessing.data_processing'
jdbc_url = f"jdbc:postgresql://{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['dbname']}"

def get_profanity_pattern():
    """Get the profanity pattern using better-profanity's word list and additional patterns."""
    profanity = Profanity()
    profanity.load_censor_words()
    
    # Add some additional phrases that might not be in the default list
    additional_phrases = {
        'wtf', 'stfu', 'bullcrap', 'dhead', 'douche'
    }
    profanity.add_censor_words(additional_phrases)
    
    # Common false positives to exclude
    false_positives = {
        'frank', 'floor', 'fridge', 'future', 'first', 'flip', 'fact', 'from', 'for',
        'food', 'free', 'front', 'frame', 'fresh', 'friday', 'friend'
    }
    
    # Get all words and escape special regex characters, excluding false positives
    all_words = [re.escape(str(word)) for word in profanity.CENSOR_WORDSET 
                 if str(word).lower() not in false_positives]
    
    # Create base pattern from words with strict word boundaries
    word_pattern = r'\b(' + '|'.join(all_words) + r')\b'
    
    # More precise patterns for common obfuscation techniques
    edge_cases = [
        # f-word variations with strict context
        r'\b[f]+[\W_]*[u]+[\W_]*[c]+[\W_]*[k]+(?:ing|er|ed)?\b',
        # "what the f" variations with mandatory following characters
        r'what\s+the\s+[f]+[\W_]*(?:[u]+[\W_]*)?[c]+[\W_]*[k]+\b',
        # "the f" variations with mandatory following characters
        r'\bthe\s+[f]+[\W_]*(?:[u]+[\W_]*)?[c]+[\W_]*[k]+\b',
        # More specific bullcrap pattern
        r'\bbull\s*cr[a@4]p\b'
    ]
    
    # Combine patterns with case insensitivity
    full_pattern = f"(?i)(?:{word_pattern}|{'|'.join(edge_cases)})"
    
    logger.info(f"Loaded {len(all_words)} profanity words and phrases")
    return full_pattern

def mask_profanity_text(text, pattern):
    """Mask profanity in text for logging purposes."""
    if not text or not isinstance(text, str):
        return text
    return re.sub(pattern, '***', text)

def get_last_processed_batch(spark, subreddit):
    """
    Gets the last processed timestamp for processed data in PostgreSQL.
    """
    try:
        # Use EXTRACT in the query to get epoch time directly
        posts_df = spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", f"(SELECT EXTRACT(EPOCH FROM MAX(created_utc)) as max_epoch FROM processed_data.posts_{subreddit.lower()}) as last_processed") \
            .option("user", DB_PARAMS["user"]) \
            .option("password", DB_PARAMS["password"]) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        
        max_timestamp = posts_df.collect()[0][0]
        return float(max_timestamp) if max_timestamp else 0
    except Exception as e:
        logger.warning(f"No existing processed data found for {subreddit}: {e}")
        return 0

def preprocess_data(spark, conn):
    """
    Preprocesses the raw Reddit data from PostgreSQL using PySpark with incremental loading.
    Only processes new data since the last processed timestamp.
    """
    # Initialize profanity filtering
    profanity_pattern = get_profanity_pattern()
    logger.info("Initialized profanity filter pattern")
    
    for subreddit in SUBREDDITS:
        current_table_name = f"raw_data.raw_{subreddit.lower()}"
        logger.info(f"Processing data for subreddit: {subreddit}")

        # Get last processed timestamp
        last_processed = get_last_processed_batch(spark, subreddit)
        
        # Read only new raw data
        df = spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", f"(SELECT * FROM {current_table_name} WHERE EXTRACT(EPOCH FROM created_utc) > {last_processed}) as new_data") \
            .option("user", DB_PARAMS["user"]) \
            .option("password", DB_PARAMS["password"]) \
            .option("driver", "org.postgresql.Driver") \
            .load()

        if df.count() == 0:
            logger.info(f"No new data to process for {subreddit}")
            continue

        # Process posts
        df_posts_filtered = df.filter(
            (F.col("title").isNotNull()) & 
            (F.col("title") != "") & 
            (F.col("title") != "[deleted]") &
            (F.col("author").isNotNull()) & 
            (F.col("author") != "") & 
            (F.col("author") != "[deleted]")
        ).dropDuplicates(["post_id"])

        # Log posts before profanity censoring
        total_posts = df_posts_filtered.count()
        
        # Censor profanity in posts
        df_posts_filtered = df_posts_filtered.withColumn(
            "title",
            F.regexp_replace(F.col("title"), profanity_pattern, "***")
        )
        
        logger.info(f"Censored profanity in {total_posts} posts for {subreddit}")
        df_posts_filtered = df_posts_filtered.drop("comments")

        # Process comments
        df_cleaned = df.withColumn("comments", F.regexp_replace(F.col("comments"), r'[\n\r\t]', ''))

        comments_schema = ArrayType(StructType([
            StructField("body", StringType(), True),
            StructField("author", StringType(), True),
            StructField("comment_id", StringType(), True),
            StructField("created_utc", DoubleType(), True)
        ]))

        # Parse and explode comments
        comments_parsed_df = df_cleaned.withColumn(
            "comments_parsed",
            F.when(F.col("comments").isNotNull() & (F.col("comments") != ""),
                F.from_json(F.col("comments"), comments_schema))
            .otherwise(F.array())
        )

        comments_df = comments_parsed_df.withColumn("comment", F.explode(F.col("comments_parsed"))) \
            .select(
                F.col("post_id"),
                F.col("comment.body").alias("body"),
                F.col("comment.author").alias("author"),
                F.col("comment.comment_id").alias("comment_id"),
                F.to_timestamp(F.col("comment.created_utc")).alias("created_utc")
            )

        # Log comments before profanity censoring
        total_comments = comments_df.count()
        
        # Censor profanity in comments
        comments_filtered_df = comments_df.withColumn(
            "body",
            F.regexp_replace(F.col("body"), profanity_pattern, "***")
        ).dropDuplicates(["comment_id"])
        
        logger.info(f"Censored profanity in {total_comments} comments for {subreddit}")

        # Save processed data
        df_posts_filtered.write.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", f"processed_data.posts_{subreddit.lower()}") \
            .option("user", DB_PARAMS["user"]) \
            .option("password", DB_PARAMS["password"]) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

        comments_filtered_df.write.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", f"processed_data.comments_{subreddit.lower()}") \
            .option("user", DB_PARAMS["user"]) \
            .option("password", DB_PARAMS["password"]) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

        logger.info(f"New data for {subreddit} has been successfully processed and appended to PostgreSQL.")