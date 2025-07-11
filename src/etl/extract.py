"""
ETL Extract module for data engineering pipeline.
Handles data extraction from various sources.
"""
import logging
import requests
import pandas as pd
from typing import Dict, Any, List
from datetime import datetime

logger = logging.getLogger(__name__)


class DataExtractor:
    """Handles data extraction from various sources."""
    
    def __init__(self):
        """Initialize the data extractor."""
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Data-Engineering-Pipeline/1.0'
        })
    
    def extract_from_api(self, url: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Extract data from a REST API endpoint."""
        try:
            logger.info(f"Extracting data from API: {url}")
            response = self.session.get(url, params=params or {})
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"Successfully extracted {len(data)} records from API")
            return data
            
        except requests.RequestException as e:
            logger.error(f"Failed to extract data from API {url}: {e}")
            raise
    
    def extract_sample_sales_data(self) -> List[Dict[str, Any]]:
        """Generate sample sales data for demonstration purposes."""
        logger.info("Generating sample sales data")
        
        import random
        from datetime import datetime, timedelta
        
        products = [
            "Laptop", "Desktop", "Mouse", "Keyboard", "Monitor", 
            "Printer", "Webcam", "Headphones", "Tablet", "Smartphone"
        ]
        regions = ["North", "South", "East", "West", "Central"]
        
        sample_data = []
        base_date = datetime.now() - timedelta(days=30)
        
        for i in range(100):
            sale_date = base_date + timedelta(days=random.randint(0, 30))
            product = random.choice(products)
            
            # Generate realistic prices based on product type
            price_ranges = {
                "Laptop": (800, 2000),
                "Desktop": (600, 1500),
                "Monitor": (200, 800),
                "Printer": (150, 500),
                "Tablet": (300, 1000),
                "Smartphone": (400, 1200),
                "Mouse": (20, 100),
                "Keyboard": (50, 200),
                "Webcam": (50, 300),
                "Headphones": (30, 400)
            }
            
            min_price, max_price = price_ranges.get(product, (50, 500))
            price = round(random.uniform(min_price, max_price), 2)
            
            sample_data.append({
                "product_name": product,
                "sales_amount": price,
                "sale_date": sale_date.strftime("%Y-%m-%d"),
                "region": random.choice(regions),
                "customer_id": f"CUST_{random.randint(1000, 9999)}",
                "quantity": random.randint(1, 5)
            })
        
        logger.info(f"Generated {len(sample_data)} sample sales records")
        return sample_data
    
    def extract_from_csv(self, file_path: str) -> pd.DataFrame:
        """Extract data from CSV file."""
        try:
            logger.info(f"Extracting data from CSV: {file_path}")
            df = pd.read_csv(file_path)
            logger.info(f"Successfully extracted {len(df)} records from CSV")
            return df
            
        except Exception as e:
            logger.error(f"Failed to extract data from CSV {file_path}: {e}")
            raise
    
    def extract_public_api_data(self) -> List[Dict[str, Any]]:
        """Extract data from a public API for demonstration."""
        try:
            # Example: JSONPlaceholder API for demonstration
            url = "https://jsonplaceholder.typicode.com/posts"
            return self.extract_from_api(url)
            
        except Exception as e:
            logger.warning(f"Failed to extract from public API: {e}")
            # Return empty list if external API fails
            return []


def get_extractor() -> DataExtractor:
    """Get a configured data extractor instance."""
    return DataExtractor()
