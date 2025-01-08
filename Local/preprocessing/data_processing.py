"""
Data Processing Module

This module handles the preprocessing of raw Reddit data using PySpark.
It includes functions for cleaning and transforming posts and comments data.

Owner: Sulman Khan
"""

import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType, DoubleType, BooleanType
import logging
from config.config import DB_PARAMS, SUBREDDITS
import re


jdbc_url = f"jdbc:postgresql://{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['dbname']}"

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
        logging.warning(f"No existing processed data found for {subreddit}: {e}")
        return 0

def filter_profanity(text: str) -> bool:
    """
    Check if text contains profanity.
    Returns True if text is clean, False if it contains profanity.
    """
    if not text or not isinstance(text, str):
        return True
        
    profanity_patterns = [
        r'(?i)\b(fuck|shit|damn|bitch|cunt|ass)\b',
        r'(?i)\b(f\*\*k|sh\*t|b\*tch|a\*\*)\b',  # Common obfuscated versions
        r'(?i)f[^\w]?c[^\w]?k',  # Attempts to bypass with special chars
        r'(?i)s[^\w]?h[^\w]?i[^\w]?t',
    ]
    
    return not any(re.search(pattern, text) for pattern in profanity_patterns)

def preprocess_data(spark, conn):
    """
    Preprocesses the raw Reddit data from PostgreSQL using PySpark with incremental loading.
    Only processes new data since the last processed timestamp.
    """
    for subreddit in SUBREDDITS:
        current_table_name = f"raw_data.raw_{subreddit.lower()}"
        logging.info(f"Processing data for subreddit: {subreddit}")

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
            logging.info(f"No new data to process for {subreddit}")
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

        # Combine profanity patterns into a single regex pattern
        profanity_pattern = r'(?i)\b(fuck|shit|damn|bitch|cunt|ass)\b|' + \
                           r'(?i)\b(f\*\*k|sh\*t|b\*tch|a\*\*)\b|' + \
                           r'(?i)f[^\w]?c[^\w]?k|' + \
                           r'(?i)s[^\w]?h[^\w]?i[^\w]?t'

        # Apply profanity filter to posts using rlike
        df_posts_filtered = df_posts_filtered.filter(
            ~F.col("title").rlike(profanity_pattern)
        )
        
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
                # Convert epoch timestamp to proper timestamp with timezone
                F.to_timestamp(F.col("comment.created_utc")).alias("created_utc")
            )

        # Filter and deduplicate comments
        comments_filtered_df = comments_df.filter(
            ~F.col("body").rlike(profanity_pattern)
        ).dropDuplicates(["comment_id"])

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

        logging.info(f"New data for {subreddit} has been successfully processed and appended to PostgreSQL.")