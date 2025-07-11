"""
ETL Transform module for data engineering pipeline.
Handles data transformation and cleaning.
"""
import logging
import pandas as pd
from typing import Dict, Any, List
from datetime import datetime

logger = logging.getLogger(__name__)


class DataTransformer:
    """Handles data transformation and cleaning operations."""
    
    def __init__(self):
        """Initialize the data transformer."""
        pass
    
    def clean_sales_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Clean and validate sales data."""
        logger.info(f"Starting data cleaning for {len(data)} records")
        
        cleaned_data = []
        invalid_records = 0
        
        for record in data:
            try:
                # Validate required fields
                if not all(key in record for key in ['product_name', 'sales_amount', 'sale_date']):
                    invalid_records += 1
                    continue
                
                # Clean and validate data
                cleaned_record = {
                    'product_name': str(record['product_name']).strip().title(),
                    'sales_amount': float(record['sales_amount']),
                    'sale_date': record['sale_date'],
                    'region': str(record.get('region', 'Unknown')).strip().title(),
                    'customer_id': str(record.get('customer_id', '')).strip(),
                    'quantity': int(record.get('quantity', 1))
                }
                
                # Validate business rules
                if cleaned_record['sales_amount'] <= 0:
                    invalid_records += 1
                    continue
                
                if cleaned_record['quantity'] <= 0:
                    cleaned_record['quantity'] = 1
                
                # Parse and validate date
                try:
                    datetime.strptime(cleaned_record['sale_date'], '%Y-%m-%d')
                except ValueError:
                    invalid_records += 1
                    continue
                
                cleaned_data.append(cleaned_record)
                
            except (ValueError, TypeError) as e:
                logger.warning(f"Invalid record skipped: {e}")
                invalid_records += 1
                continue
        
        logger.info(
            f"Data cleaning completed. Valid records: {len(cleaned_data)}, "
            f"Invalid records: {invalid_records}"
        )
        
        return cleaned_data
    
    def aggregate_sales_by_region(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Aggregate sales data by region."""
        logger.info("Aggregating sales data by region")
        
        region_aggregates = {}
        
        for record in data:
            region = record['region']
            if region not in region_aggregates:
                region_aggregates[region] = {
                    'region': region,
                    'total_sales': 0,
                    'total_revenue': 0.0,
                    'total_quantity': 0,
                    'product_count': 0
                }
            
            region_aggregates[region]['total_sales'] += 1
            region_aggregates[region]['total_revenue'] += record['sales_amount']
            region_aggregates[region]['total_quantity'] += record['quantity']
            region_aggregates[region]['product_count'] += 1
        
        # Calculate averages
        for region_data in region_aggregates.values():
            region_data['avg_sale_amount'] = (
                region_data['total_revenue'] / region_data['total_sales']
                if region_data['total_sales'] > 0 else 0
            )
            region_data['avg_quantity'] = (
                region_data['total_quantity'] / region_data['total_sales']
                if region_data['total_sales'] > 0 else 0
            )
        
        result = list(region_aggregates.values())
        logger.info(f"Created aggregations for {len(result)} regions")
        
        return result
    
    def aggregate_sales_by_product(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Aggregate sales data by product."""
        logger.info("Aggregating sales data by product")
        
        product_aggregates = {}
        
        for record in data:
            product = record['product_name']
            if product not in product_aggregates:
                product_aggregates[product] = {
                    'product_name': product,
                    'total_sales': 0,
                    'total_revenue': 0.0,
                    'total_quantity': 0,
                    'regions': set()
                }
            
            product_aggregates[product]['total_sales'] += 1
            product_aggregates[product]['total_revenue'] += record['sales_amount']
            product_aggregates[product]['total_quantity'] += record['quantity']
            product_aggregates[product]['regions'].add(record['region'])
        
        # Convert sets to counts and calculate averages
        result = []
        for product_data in product_aggregates.values():
            product_data['region_count'] = len(product_data['regions'])
            product_data['avg_sale_amount'] = (
                product_data['total_revenue'] / product_data['total_sales']
                if product_data['total_sales'] > 0 else 0
            )
            product_data['avg_quantity'] = (
                product_data['total_quantity'] / product_data['total_sales']
                if product_data['total_sales'] > 0 else 0
            )
            # Remove the set as it's not serializable
            del product_data['regions']
            result.append(product_data)
        
        result.sort(key=lambda x: x['total_revenue'], reverse=True)
        logger.info(f"Created aggregations for {len(result)} products")
        
        return result
    
    def add_calculated_fields(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Add calculated fields to the data."""
        logger.info("Adding calculated fields to data")
        
        for record in data:
            # Add total value (sales_amount * quantity)
            record['total_value'] = record['sales_amount'] * record['quantity']
            
            # Add month and year from sale_date
            try:
                sale_date = datetime.strptime(record['sale_date'], '%Y-%m-%d')
                record['sale_month'] = sale_date.strftime('%Y-%m')
                record['sale_year'] = sale_date.year
                record['sale_quarter'] = f"Q{(sale_date.month-1)//3 + 1}"
            except ValueError:
                record['sale_month'] = None
                record['sale_year'] = None
                record['sale_quarter'] = None
        
        logger.info("Calculated fields added successfully")
        return data


def get_transformer() -> DataTransformer:
    """Get a configured data transformer instance."""
    return DataTransformer()
