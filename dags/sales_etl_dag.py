"""
Apache Airflow DAG for Sales Data ETL Pipeline.
Orchestrates the complete ETL workflow with Supabase PostgreSQL.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import sys
import os

# Add src directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from etl import get_extractor, get_transformer, get_loader
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'sales_data_etl_pipeline',
    default_args=default_args,
    description='ETL Pipeline for Sales Data with Supabase',
    schedule_interval=timedelta(days=1),  # Run daily
    catchup=False,
    tags=['etl', 'sales', 'supabase'],
)


def extract_data(**context):
    """Extract sales data."""
    logger.info("Starting data extraction")
    extractor = get_extractor()
    
    # Extract sample data
    sample_data = extractor.extract_sample_sales_data()
    logger.info(f"Extracted {len(sample_data)} records")
    
    # Store in XCom for next task
    return sample_data


def transform_data(**context):
    """Transform and clean the extracted data."""
    logger.info("Starting data transformation")
    transformer = get_transformer()
    
    # Get data from previous task
    raw_data = context['task_instance'].xcom_pull(task_ids='extract_data')
    
    # Clean data
    cleaned_data = transformer.clean_sales_data(raw_data)
    logger.info(f"Cleaned {len(cleaned_data)} records")
    
    # Add calculated fields
    enhanced_data = transformer.add_calculated_fields(cleaned_data)
    logger.info("Added calculated fields")
    
    # Create aggregations
    region_aggregates = transformer.aggregate_sales_by_region(enhanced_data)
    product_aggregates = transformer.aggregate_sales_by_product(enhanced_data)
    
    logger.info(f"Created {len(region_aggregates)} regional aggregates")
    logger.info(f"Created {len(product_aggregates)} product aggregates")
    
    return {
        'sales_data': enhanced_data,
        'region_aggregates': region_aggregates,
        'product_aggregates': product_aggregates
    }


def load_data(**context):
    """Load transformed data into Supabase."""
    logger.info("Starting data loading")
    loader = get_loader()
    
    # Get data from previous task
    transformed_data = context['task_instance'].xcom_pull(task_ids='transform_data')
    
    # Create tables
    loader.create_tables()
    logger.info("Database tables created/verified")
    
    # Load data
    sales_loaded = loader.load_sales_data(transformed_data['sales_data'])
    region_loaded = loader.load_region_aggregates(transformed_data['region_aggregates'])
    product_loaded = loader.load_product_aggregates(transformed_data['product_aggregates'])
    
    logger.info(f"Loaded {sales_loaded} sales records")
    logger.info(f"Loaded {region_loaded} regional aggregates")
    logger.info(f"Loaded {product_loaded} product aggregates")
    
    # Generate summary
    summary = loader.get_data_summary()
    logger.info(f"Pipeline summary: {summary}")
    
    return summary


def validate_data(**context):
    """Validate the loaded data."""
    logger.info("Starting data validation")
    loader = get_loader()
    
    summary = loader.get_data_summary()
    
    # Basic validation checks
    if summary['sales_records'] == 0:
        raise ValueError("No sales records found in database")
    
    if summary['total_revenue'] <= 0:
        raise ValueError("Total revenue should be positive")
    
    if summary['regions'] == 0:
        raise ValueError("No regional data found")
    
    if summary['products'] == 0:
        raise ValueError("No product data found")
    
    logger.info("Data validation passed")
    return "Data validation successful"


# Define tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    dag=dag,
)

# Health check task
health_check_task = BashOperator(
    task_id='health_check',
    bash_command='echo "ETL Pipeline completed successfully at $(date)"',
    dag=dag,
)

# Define task dependencies
extract_task >> transform_task >> load_task >> validate_task >> health_check_task
