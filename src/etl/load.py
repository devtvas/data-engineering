"""
ETL Load module for data engineering pipeline.
Handles data loading into Supabase PostgreSQL.
"""
import logging
from typing import Dict, Any, List
from src.database.connection import get_db_connection

logger = logging.getLogger(__name__)


class DataLoader:
    """Handles data loading into the database."""
    
    def __init__(self):
        """Initialize the data loader."""
        self.db = get_db_connection()
    
    def create_tables(self):
        """Create necessary tables in the database."""
        logger.info("Creating database tables")
        
        # Sales data table
        sales_table_query = """
        CREATE TABLE IF NOT EXISTS sales_data (
            id SERIAL PRIMARY KEY,
            product_name VARCHAR(100) NOT NULL,
            sales_amount DECIMAL(10, 2) NOT NULL,
            sale_date DATE NOT NULL,
            region VARCHAR(50),
            customer_id VARCHAR(50),
            quantity INTEGER DEFAULT 1,
            total_value DECIMAL(10, 2),
            sale_month VARCHAR(7),
            sale_year INTEGER,
            sale_quarter VARCHAR(2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # Regional aggregates table
        region_agg_table_query = """
        CREATE TABLE IF NOT EXISTS region_aggregates (
            id SERIAL PRIMARY KEY,
            region VARCHAR(50) NOT NULL UNIQUE,
            total_sales INTEGER NOT NULL,
            total_revenue DECIMAL(12, 2) NOT NULL,
            total_quantity INTEGER NOT NULL,
            product_count INTEGER NOT NULL,
            avg_sale_amount DECIMAL(10, 2),
            avg_quantity DECIMAL(8, 2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # Product aggregates table
        product_agg_table_query = """
        CREATE TABLE IF NOT EXISTS product_aggregates (
            id SERIAL PRIMARY KEY,
            product_name VARCHAR(100) NOT NULL UNIQUE,
            total_sales INTEGER NOT NULL,
            total_revenue DECIMAL(12, 2) NOT NULL,
            total_quantity INTEGER NOT NULL,
            region_count INTEGER NOT NULL,
            avg_sale_amount DECIMAL(10, 2),
            avg_quantity DECIMAL(8, 2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # Execute table creation queries
        try:
            self.db.execute_command(sales_table_query)
            self.db.execute_command(region_agg_table_query)
            self.db.execute_command(product_agg_table_query)
            logger.info("Database tables created successfully")
        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            raise
    
    def load_sales_data(self, data: List[Dict[str, Any]]) -> int:
        """Load sales data into the database."""
        logger.info(f"Loading {len(data)} sales records into database")
        
        insert_query = """
        INSERT INTO sales_data 
        (product_name, sales_amount, sale_date, region, customer_id, 
         quantity, total_value, sale_month, sale_year, sale_quarter)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
        """
        
        loaded_count = 0
        try:
            for record in data:
                self.db.execute_command(insert_query, (
                    record['product_name'],
                    record['sales_amount'],
                    record['sale_date'],
                    record['region'],
                    record.get('customer_id', ''),
                    record['quantity'],
                    record.get('total_value', record['sales_amount'] * record['quantity']),
                    record.get('sale_month'),
                    record.get('sale_year'),
                    record.get('sale_quarter')
                ))
                loaded_count += 1
            
            logger.info(f"Successfully loaded {loaded_count} sales records")
            return loaded_count
            
        except Exception as e:
            logger.error(f"Failed to load sales data: {e}")
            raise
    
    def load_region_aggregates(self, data: List[Dict[str, Any]], confirm_delete: bool = False) -> int:
        """Load regional aggregates into the database.
        
        Args:
            data (List[Dict[str, Any]]): List of regional aggregate records to load.
            confirm_delete (bool): If True, clears existing data in the region_aggregates table.
        """
        logger.info(f"Loading {len(data)} regional aggregates into database")
        
        # Clear existing data if confirmed
        if confirm_delete:
            logger.warning("Clearing all data from region_aggregates table")
            self.db.execute_command("DELETE FROM region_aggregates;")
        else:
            logger.info("Skipping deletion of existing data in region_aggregates table")
        
        insert_query = """
        INSERT INTO region_aggregates 
        (region, total_sales, total_revenue, total_quantity, 
         product_count, avg_sale_amount, avg_quantity)
        VALUES (%s, %s, %s, %s, %s, %s, %s);
        """
        
        loaded_count = 0
        try:
            for record in data:
                self.db.execute_command(insert_query, (
                    record['region'],
                    record['total_sales'],
                    record['total_revenue'],
                    record['total_quantity'],
                    record['product_count'],
                    record['avg_sale_amount'],
                    record['avg_quantity']
                ))
                loaded_count += 1
            
            logger.info(f"Successfully loaded {loaded_count} regional aggregates")
            return loaded_count
            
        except Exception as e:
            logger.error(f"Failed to load regional aggregates: {e}")
            raise
    
    def load_product_aggregates(self, data: List[Dict[str, Any]], confirm_delete: bool = True) -> int:
        """Load product aggregates into the database.
        
        Args:
            data (List[Dict[str, Any]]): List of product aggregate records to load.
            confirm_delete (bool): If True, clears existing data in the product_aggregates table. Defaults to True.
        """
        logger.info(f"Loading {len(data)} product aggregates into database")
        
        # Clear existing data
        if confirm_delete:
            logger.warning("Deleting all data from product_aggregates table. This operation is destructive.")
            self.db.execute_command("DELETE FROM product_aggregates;")
        else:
            logger.error("Delete operation not confirmed. Aborting.")
            raise ValueError("Delete operation requires explicit confirmation.")
        
        insert_query = """
        INSERT INTO product_aggregates 
        (product_name, total_sales, total_revenue, total_quantity, 
         region_count, avg_sale_amount, avg_quantity)
        VALUES (%s, %s, %s, %s, %s, %s, %s);
        """
        
        loaded_count = 0
        try:
            for record in data:
                self.db.execute_command(insert_query, (
                    record['product_name'],
                    record['total_sales'],
                    record['total_revenue'],
                    record['total_quantity'],
                    record['region_count'],
                    record['avg_sale_amount'],
                    record['avg_quantity']
                ))
                loaded_count += 1
            
            logger.info(f"Successfully loaded {loaded_count} product aggregates")
            return loaded_count
            
        except Exception as e:
            logger.error(f"Failed to load product aggregates: {e}")
            raise
    
    def get_data_summary(self) -> Dict[str, Any]:
        """Get a summary of data in the database."""
        logger.info("Generating data summary")
        
        try:
            # Count records in each table
            sales_count = self.db.execute_query("SELECT COUNT(*) as count FROM sales_data;")[0]['count']
            region_count = self.db.execute_query("SELECT COUNT(*) as count FROM region_aggregates;")[0]['count']
            product_count = self.db.execute_query("SELECT COUNT(*) as count FROM product_aggregates;")[0]['count']
            
            # Get total revenue
            total_revenue_result = self.db.execute_query(
                "SELECT COALESCE(SUM(sales_amount), 0) as total_revenue FROM sales_data;"
            )
            total_revenue = total_revenue_result[0]['total_revenue']
            
            summary = {
                'sales_records': sales_count,
                'regions': region_count,
                'products': product_count,
                'total_revenue': float(total_revenue) if total_revenue else 0.0
            }
            
            logger.info(f"Data summary generated: {summary}")
            return summary
            
        except Exception as e:
            logger.error(f"Failed to generate data summary: {e}")
            raise


def get_loader() -> DataLoader:
    """Get a configured data loader instance."""
    return DataLoader()
