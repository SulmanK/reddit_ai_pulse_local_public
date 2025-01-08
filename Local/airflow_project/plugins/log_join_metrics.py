import logging
import re
import time
import statsd
import psycopg2
from datetime import datetime
import sys
import os

# Add the project root directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from config.config import get_database_config

# Initialize StatsD client
statsd_client = statsd.StatsClient(
    host='statsd-exporter',
    port=9125,
    prefix='airflow.reddit'
)

def get_log_path(task_instance, task_id='run_dbt_join_summary_analysis'):
    """Construct the log file path from task instance"""
    dag_id = task_instance.dag_id
    execution_date = task_instance.execution_date.strftime('%Y-%m-%dT%H:%M:%S.%f') + '+00:00'
    attempt = task_instance.try_number
    
    return f"/opt/airflow/logs/dag_id={dag_id}/run_id=manual__{execution_date}/task_id={task_id}/attempt={attempt}.log"

def extract_join_metrics(log_path):
    """Extract metrics from join operation logs and database"""
    metrics = {
        'rows_joined': 0,
        'attempt': int(re.search(r"attempt=(\d+)", log_path).group(1)),
        'duration_seconds': 0
    }
    
    try:
        # Get database metrics
        db_config = get_database_config()
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        
        # Query for row count with correct schema
        cur.execute("""
            SELECT COUNT(*) 
            FROM processed_data.joined_summary_analysis
        """)
        metrics['rows_joined'] = cur.fetchone()[0]
        
        # Parse logs for timing and success/failure
        with open(log_path, 'r') as f:
            log_content = f.readlines()
            
        success = False
        
        for line in log_content:
            # Look for DBT completion message with duration
            if "Finished running 1 table model in" in line:
                duration_match = re.search(r"(\d+\.\d+)s", line)
                if duration_match:
                    metrics['duration_seconds'] = float(duration_match.group(1))
                    statsd_client.timing('join.duration', metrics['duration_seconds'] * 1000)
            
            # Check for successful completion
            elif "Completed successfully" in line:
                success = True
        
        metrics['next_attempt'] = 1 if success else metrics['attempt'] + 1
        
        # Send metrics to StatsD
        statsd_client.gauge('join.rows_processed', metrics['rows_joined'])
        statsd_client.gauge('join.current_attempt', metrics['attempt'])
        
        if success:
            statsd_client.incr('join.success')
        else:
            statsd_client.incr('join.retry')
        
        logging.info(f"""
        Join Operation Metrics Summary:
        - Rows in Joined Table: {metrics['rows_joined']}
        - Current Attempt: {metrics['attempt']}
        - Next Attempt Should Be: {metrics['next_attempt']}
        - Duration: {metrics['duration_seconds']:.2f} seconds
        """)
        
        return metrics
        
    except Exception as e:
        statsd_client.incr('join.error')
        logging.error(f"Error parsing join metrics: {str(e)}")
        raise e
    
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close() 