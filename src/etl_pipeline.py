"""
Complete ETL pipeline demonstrating data engineering workflow.
"""
import logging
from src.etl import get_extractor, get_transformer, get_loader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_etl_pipeline():
    """Run the complete ETL pipeline."""
    logger.info("Starting ETL Pipeline")
    
    try:
        # Initialize ETL components
        extractor = get_extractor()
        transformer = get_transformer()
        loader = get_loader()
        
        # Step 1: Extract
        logger.info("=== EXTRACT PHASE ===")
        raw_data = extractor.extract_sample_sales_data()
        logger.info(f"Extracted {len(raw_data)} raw records")
        
        # Step 2: Transform
        logger.info("=== TRANSFORM PHASE ===")
        
        # Clean data
        cleaned_data = transformer.clean_sales_data(raw_data)
        logger.info(f"Cleaned data: {len(cleaned_data)} valid records")
        
        # Add calculated fields
        enhanced_data = transformer.add_calculated_fields(cleaned_data)
        logger.info("Added calculated fields to data")
        
        # Create aggregations
        region_aggregates = transformer.aggregate_sales_by_region(
            enhanced_data
        )
        product_aggregates = transformer.aggregate_sales_by_product(
            enhanced_data
        )
        
        logger.info(f"Created {len(region_aggregates)} regional aggregates")
        logger.info(f"Created {len(product_aggregates)} product aggregates")
        
        # Step 3: Load
        logger.info("=== LOAD PHASE ===")
        
        # Create tables
        loader.create_tables()
        
        # Load data
        sales_loaded = loader.load_sales_data(enhanced_data)
        region_loaded = loader.load_region_aggregates(region_aggregates)
        product_loaded = loader.load_product_aggregates(product_aggregates)
        
        logger.info(f"Loaded {sales_loaded} sales records")
        logger.info(f"Loaded {region_loaded} regional aggregates")
        logger.info(f"Loaded {product_loaded} product aggregates")
        
        # Generate summary
        summary = loader.get_data_summary()
        logger.info("=== PIPELINE SUMMARY ===")
        logger.info(f"Total sales records: {summary['sales_records']}")
        logger.info(f"Total regions: {summary['regions']}")
        logger.info(f"Total products: {summary['products']}")
        logger.info(f"Total revenue: ${summary['total_revenue']:.2f}")
        
        logger.info("ETL Pipeline completed successfully!")
        return summary
        
    except Exception as e:
        logger.error(f"ETL Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    run_etl_pipeline()
