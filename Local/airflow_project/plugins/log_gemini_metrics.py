import logging
import re
import statsd
import sys
import os

# Initialize StatsD client
statsd_client = statsd.StatsClient(
    host='statsd-exporter',
    port=9125,
    prefix='airflow.reddit'
)

def get_log_path(task_instance, task_id='run_gemini'):
    """Construct the log file path from task instance"""
    dag_id = task_instance.dag_id
    execution_date = task_instance.execution_date.strftime('%Y-%m-%dT%H:%M:%S.%f') + '+00:00'
    attempt = task_instance.try_number
    
    return f"/opt/airflow/logs/dag_id={dag_id}/run_id=manual__{execution_date}/task_id={task_id}/attempt={attempt}.log"

def extract_gemini_metrics(log_path):
    """Extract metrics from Gemini task logs"""
    metrics = {
        'outputs_generated': 0,
        'attempt': int(re.search(r"attempt=(\d+)", log_path).group(1)),
        'duration_seconds': 0,
        'subreddits_processed': 0
    }
    
    try:
        with open(log_path, 'r') as f:
            log_content = f.readlines()
            
        start_time = None
        end_time = None
        success = False
        
        for line in log_content:
            # Count subreddits being analyzed
            if "Analyzing data for subreddit:" in line:
                metrics['subreddits_processed'] += 1
            
            # Count successful outputs
            elif "Output saved to" in line:
                metrics['outputs_generated'] += 1
            
            # Track task timing
            if "Analyzing data for subreddit:" in line and not start_time:
                start_time = re.search(r"\[(.*?)\]", line)
                if start_time:
                    start_time = start_time.group(1)
            
            # Get last timestamp as end time
            timestamp = re.search(r"\[(.*?)\]", line)
            if timestamp:
                end_time = timestamp.group(1)
        
        # Calculate duration if we have both timestamps
        if start_time and end_time:
            from datetime import datetime
            start_dt = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S.%f%z")
            end_dt = datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%S.%f%z")
            metrics['duration_seconds'] = (end_dt - start_dt).total_seconds()
        
        success = metrics['outputs_generated'] == metrics['subreddits_processed']
        metrics['next_attempt'] = 1 if success else metrics['attempt'] + 1
        
        # Send metrics to StatsD
        statsd_client.gauge('gemini.outputs_generated', metrics['outputs_generated'])
        statsd_client.gauge('gemini.subreddits_processed', metrics['subreddits_processed'])
        statsd_client.gauge('gemini.current_attempt', metrics['attempt'])
        statsd_client.timing('gemini.duration', metrics['duration_seconds'] * 1000)
        
        if success:
            statsd_client.incr('gemini.success')
        else:
            statsd_client.incr('gemini.retry')
        
        logging.info(f"""
        Gemini Task Metrics Summary:
        - Subreddits Processed: {metrics['subreddits_processed']}
        - Outputs Generated: {metrics['outputs_generated']}
        - Current Attempt: {metrics['attempt']}
        - Next Attempt Should Be: {metrics['next_attempt']}
        - Duration: {metrics['duration_seconds']:.2f} seconds
        """)
        
        return metrics
        
    except Exception as e:
        statsd_client.incr('gemini.error')
        logging.error(f"Error parsing Gemini metrics: {str(e)}")
        raise e 