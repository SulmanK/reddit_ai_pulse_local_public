import logging
import re
import time
import statsd
from datetime import datetime

# Initialize StatsD client
statsd_client = statsd.StatsClient(
    host='statsd-exporter',
    port=9125,
    prefix='airflow.reddit'
)

def get_log_path(task_instance, task_id='ingest_and_preprocess'):
    """Construct the log file path from task instance"""
    dag_id = task_instance.dag_id
    execution_date = task_instance.execution_date.strftime('%Y-%m-%dT%H:%M:%S.%f') + '+00:00'
    attempt = task_instance.try_number
    
    return f"/opt/airflow/logs/dag_id={dag_id}/run_id=manual__{execution_date}/task_id={task_id}/attempt={attempt}.log"

def extract_metrics_from_log(log_path):
    """Extract metrics from ingest and preprocess task logs"""
    metrics = {
        'processed_posts': 0,
        'total_summaries': 0,
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
            # Look for timestamps with new format
            timestamp_match = re.search(r"\[(.*?)\]", line)
            
            # Update start time condition to match actual log
            if "Starting attempt" in line and timestamp_match:
                try:
                    start_time = datetime.strptime(timestamp_match.group(1).strip(), "%Y-%m-%d, %H:%M:%S UTC")
                except ValueError:
                    start_time = datetime.strptime(timestamp_match.group(1).strip(), "%Y-%m-%dT%H:%M:%S.%f%z")
            
            # Update end time condition to match actual log
            elif "Task exited with return code 0" in line and timestamp_match:
                try:
                    end_time = datetime.strptime(timestamp_match.group(1).strip(), "%Y-%m-%d, %H:%M:%S UTC")
                except ValueError:
                    end_time = datetime.strptime(timestamp_match.group(1).strip(), "%Y-%m-%dT%H:%M:%S.%f%z")
                success = True
            
            elif "Successfully processed post" in line:
                metrics['processed_posts'] += 1
            
            elif "Processing complete. Total new summaries added:" in line:
                summaries = re.search(r"Total new summaries added: (\d+)", line)
                if summaries:
                    metrics['total_summaries'] = int(summaries.group(1))
        
        # Calculate duration if we have both timestamps
        if start_time and end_time:
            metrics['duration_seconds'] = (end_time - start_time).total_seconds()
            statsd_client.timing('ingest_preprocess.duration', metrics['duration_seconds'] * 1000)
        
        metrics['next_attempt'] = 1 if success else metrics['attempt'] + 1
        
        # Send metrics using the StatsD client
        statsd_client.gauge('ingest_preprocess.processed_posts', metrics['processed_posts'])
        statsd_client.gauge('ingest_preprocess.total_summaries', metrics['total_summaries'])
        statsd_client.gauge('ingest_preprocess.current_attempt', metrics['attempt'])
        
        if success:
            statsd_client.incr('ingest_preprocess.success')
        else:
            statsd_client.incr('ingest_preprocess.retry')
        
        logging.info(f"""
        Ingest and Preprocess Metrics Summary:
        - Total Posts Processed: {metrics['processed_posts']}
        - Total Summaries Added: {metrics['total_summaries']}
        - Current Attempt: {metrics['attempt']}
        - Next Attempt Should Be: {metrics['next_attempt']}
        - Duration: {metrics['duration_seconds']:.2f} seconds
        """)
        
        return metrics
        
    except Exception as e:
        statsd_client.incr('ingest_preprocess.error')
        logging.error(f"Error parsing ingest_preprocess metrics from logs: {str(e)}")
        raise e 