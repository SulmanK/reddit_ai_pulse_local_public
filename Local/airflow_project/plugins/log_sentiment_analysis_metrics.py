import logging
import re
import time
import statsd
from datetime import datetime
from Local.airflow_project.plugins.logging_utils import get_log_path

# Initialize StatsD client
statsd_client = statsd.StatsClient(
    host='statsd-exporter',
    port=9125,
    prefix='airflow.reddit'
)

def extract_sentiment_metrics(log_path):
    """Extract metrics from sentiment analysis task logs"""
    metrics = {
        'sentiments_processed': 0,
        'attempt': int(re.search(r"attempt=(\d+)", log_path).group(1)),
        'duration_seconds': 0
    }
    
    try:
        with open(log_path, 'r') as f:
            log_content = f.readlines()
            
        success = False
        start_time = None
        end_time = None
        
        for line in log_content:
            timestamp_match = re.search(r"\[(.*?)\]", line)
            
            # Look for start time
            if "Emotion analysis model loaded" in line and timestamp_match:
                try:
                    start_time = datetime.strptime(timestamp_match.group(1).strip(), "%Y-%m-%d, %H:%M:%S UTC")
                except ValueError:
                    start_time = datetime.strptime(timestamp_match.group(1).strip(), "%Y-%m-%dT%H:%M:%S.%f%z")
            
            # Count processed sentiments - updated trigger
            elif "Sentiment analysis added for comment_id:" in line:
                metrics['sentiments_processed'] += 1
            
            # Look for completion
            elif "Database connection closed" in line and timestamp_match:
                try:
                    end_time = datetime.strptime(timestamp_match.group(1).strip(), "%Y-%m-%d, %H:%M:%S UTC")
                except ValueError:
                    end_time = datetime.strptime(timestamp_match.group(1).strip(), "%Y-%m-%dT%H:%M:%S.%f%z")
                success = True
        
        # Calculate duration if we have both timestamps
        if start_time and end_time:
            metrics['duration_seconds'] = (end_time - start_time).total_seconds()
            statsd_client.timing('sentiment.duration', metrics['duration_seconds'] * 1000)
        
        metrics['next_attempt'] = 1 if success else metrics['attempt'] + 1
        
        # Send metrics to StatsD
        statsd_client.gauge('sentiment.processed', metrics['sentiments_processed'])
        statsd_client.gauge('sentiment.current_attempt', metrics['attempt'])
        
        if success:
            statsd_client.incr('sentiment.success')
        else:
            statsd_client.incr('sentiment.retry')
        
        logging.info(f"""
        Sentiment Analysis Metrics Summary:
        - Sentiments Processed: {metrics['sentiments_processed']}
        - Current Attempt: {metrics['attempt']}
        - Next Attempt Should Be: {metrics['next_attempt']}
        - Duration: {metrics['duration_seconds']:.2f} seconds
        """)
        
        return metrics
        
    except Exception as e:
        statsd_client.incr('sentiment.error')
        logging.error(f"Error parsing sentiment analysis metrics from logs: {str(e)}")
        raise e 