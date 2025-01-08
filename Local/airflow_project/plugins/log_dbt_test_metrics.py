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

def get_log_path(task_instance, task_id):
    """Construct the log file path from task instance"""
    dag_id = task_instance.dag_id
    execution_date = task_instance.execution_date.strftime('%Y-%m-%dT%H:%M:%S.%f') + '+00:00'
    attempt = task_instance.try_number
    
    return f"/opt/airflow/logs/dag_id={dag_id}/run_id=manual__{execution_date}/task_id={task_id}/attempt={attempt}.log"

def extract_dbt_test_metrics(log_path):
    """Extract DBT test metrics from logs for any DBT task"""
    task_name = re.search(r"task_id=(.*?)/", log_path).group(1)
    metrics = {
        'tests_passed': 0,
        'tests_failed': 0,
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
            
            if "Running with dbt" in line and timestamp_match:
                try:
                    start_time = datetime.strptime(timestamp_match.group(1).strip(), "%Y-%m-%d, %H:%M:%S UTC")
                except ValueError:
                    start_time = datetime.strptime(timestamp_match.group(1).strip(), "%Y-%m-%dT%H:%M:%S.%f%z")
            
            elif "PASS" in line:
                metrics['tests_passed'] += 1
            elif "FAIL" in line:
                metrics['tests_failed'] += 1
            
            elif "Completed successfully" in line and timestamp_match:
                try:
                    end_time = datetime.strptime(timestamp_match.group(1).strip(), "%Y-%m-%d, %H:%M:%S UTC")
                except ValueError:
                    end_time = datetime.strptime(timestamp_match.group(1).strip(), "%Y-%m-%dT%H:%M:%S.%f%z")
                success = True
        
        if start_time and end_time:
            metrics['duration_seconds'] = (end_time - start_time).total_seconds()
            statsd_client.timing(f'dbt.{task_name}.duration', metrics['duration_seconds'] * 1000)
        
        metrics['next_attempt'] = 1 if success else metrics['attempt'] + 1
        metrics['total_tests'] = metrics['tests_passed'] + metrics['tests_failed']
        
        # Send metrics with task-specific names
        statsd_client.gauge(f'dbt.{task_name}.tests.passed', metrics['tests_passed'])
        statsd_client.gauge(f'dbt.{task_name}.tests.failed', metrics['tests_failed'])
        statsd_client.gauge(f'dbt.{task_name}.tests.total', metrics['total_tests'])
        statsd_client.gauge(f'dbt.{task_name}.current_attempt', metrics['attempt'])
        
        if success:
            statsd_client.incr(f'dbt.{task_name}.success')
        else:
            statsd_client.incr(f'dbt.{task_name}.retry')
        
        logging.info(f"""
        DBT {task_name} Metrics Summary:
        - Tests Passed: {metrics['tests_passed']}
        - Tests Failed: {metrics['tests_failed']}
        - Total Tests: {metrics['total_tests']}
        - Current Attempt: {metrics['attempt']}
        - Next Attempt Should Be: {metrics['next_attempt']}
        - Duration: {metrics['duration_seconds']:.2f} seconds
        """)
        
        return metrics
        
    except Exception as e:
        statsd_client.incr(f'dbt.{task_name}.error')
        logging.error(f"Error parsing DBT metrics for {task_name}: {str(e)}")
        raise e 