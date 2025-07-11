"""
Main entry point for the data engineering pipeline.
Demonstrates Supabase PostgreSQL connection and basic operations.
"""
import logging
import sys
import os

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.connection import get_db_connection

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Main function to demonstrate database connectivity and operations."""
    logger.info("Starting Data Engineering Pipeline")
    
    # Get database connection
    db = get_db_connection()
    
    # Test connection
    logger.info("Testing database connection...")
    if not db.test_connection():
        logger.error(
            "Database connection failed. Please check your configuration."
        )
        return
    
    # Example: Basic database operations
    try:
        # Create a sample table if it doesn't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS sales_data (
            id SERIAL PRIMARY KEY,
            product_name VARCHAR(100) NOT NULL,
            sales_amount DECIMAL(10, 2) NOT NULL,
            sale_date DATE NOT NULL,
            region VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        db.execute_command(create_table_query)
        logger.info("Sales table created/verified successfully")
        
        # Insert sample data
        sample_data = [
            ("Laptop", 1299.99, "2024-01-15", "North"),
            ("Mouse", 29.99, "2024-01-16", "South"),
            ("Keyboard", 79.99, "2024-01-17", "East"),
            ("Monitor", 299.99, "2024-01-18", "West")
        ]
        
        for product, amount, date, region in sample_data:
            insert_query = """
            INSERT INTO sales_data
            (product_name, sales_amount, sale_date, region)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
            """
            db.execute_command(insert_query, (product, amount, date, region))
        
        logger.info("Sample data inserted successfully")
        
        # Query data
        select_query = """
        SELECT
            product_name,
            sales_amount,
            sale_date,
            region,
            created_at
        FROM sales_data
        ORDER BY sale_date DESC
        LIMIT 10;
        """
        
        results = db.execute_query(select_query)
        
        logger.info(f"Retrieved {len(results)} records:")
        for row in results:
            logger.info(
                f"  {row['product_name']} - "
                f"${row['sales_amount']} - {row['region']}"
            )
        
        # Example aggregation query
        aggregation_query = """
        SELECT
            region,
            COUNT(*) as total_sales,
            SUM(sales_amount) as total_revenue,
            AVG(sales_amount) as avg_sale_amount
        FROM sales_data
        GROUP BY region
        ORDER BY total_revenue DESC;
        """
        
        agg_results = db.execute_query(aggregation_query)
        
        logger.info("Sales summary by region:")
        for row in agg_results:
            logger.info(
                f"  {row['region']}: {row['total_sales']} sales, "
                f"${row['total_revenue']:.2f} revenue, "
                f"${row['avg_sale_amount']:.2f} avg"
            )
        
        logger.info("Data Engineering Pipeline completed successfully!")
        
    except Exception as e:
        logger.error(f"An error occurred during pipeline execution: {e}")
        raise


if __name__ == "__main__":
    main()