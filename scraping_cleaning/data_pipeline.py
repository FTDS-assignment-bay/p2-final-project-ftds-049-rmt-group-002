"""
Data Cleaning Pipeline for Coffee Product Data

Author  : Data Engineer
Date    : 2025-12-29
Purpose : Automated, reproducible data cleaning pipeline

Usage:
    data_pipeline.py 
    --input     : tokopedia_products.csv 
    --output    : tokopedia_products_cleaned.csv

"""

import pandas as pd
import numpy as np
import re
import argparse  
import logging
from datetime import datetime
from pathlib import Path
import sys
      
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class CoffeeDataPipeline:
    """
    Automated data cleaning pipeline for coffee product data

    """
    
    def __init__(self, input_file: str, output_file: str = None):
        # Initialize pipeline
        
        self.input_file = input_file
        self.output_file = output_file or input_file.replace('.csv', '_cleaned.csv')
        self.df_raw = None
        self.df_clean = None
        self.stats = {}
        
        logger.info(f"Pipeline initialized")
        logger.info(f"Input: {self.input_file}")
        logger.info(f"Output: {self.output_file}")
    
    def load_data(self):
        # Load raw data from CSV
        
        try:
            logger.info("Loading raw data...")
            self.df_raw = pd.read_csv(self.input_file)
            logger.info(f" Loaded {len(self.df_raw):,} rows, {len(self.df_raw.columns)} columns")
            self.stats['original_rows'] = len(self.df_raw)
            return self
        except Exception as e:
            logger.error(f"Failed to load data: {e}")
            raise
    
    def remove_duplicates(self):
        # Remove duplicate rows

        logger.info("Removing duplicates...")
        initial_count = len(self.df_raw)
        self.df_raw = self.df_raw.drop_duplicates(keep='first')
        removed = initial_count - len(self.df_raw)
        
        if removed > 0:
            logger.info(f" Removed {removed:,} duplicate rows")
        else:
            logger.info(" No duplicates found")
        
        self.stats['duplicates_removed'] = removed
        return self
    
    def clean_price(self, price_str):
        # Clean and convert price string to numeric
        
        if pd.isna(price_str):
            return np.nan
        
        price_str = str(price_str)
        price_clean = re.sub(r'[Rp\s.]', '', price_str)
        price_clean = re.sub(r'[^0-9,]', '', price_clean)
        price_clean = price_clean.replace(',', '')
        
        try:
            return int(price_clean)
        except:
            return np.nan
    
    def clean_text(self, text):
    
        # Clean text fields
        if pd.isna(text):
            return text
        
        text = str(text)
        text = re.sub(r'\s+', ' ', text)
        text = text.strip()
        
        return text
    
    def is_coffee_product(self, product_name):
        """
        Classify whether a product is coffee-related
            
        Returns:
            bool: True if coffee product, False otherwise

        """
        if pd.isna(product_name):
            return False
        
        name_lower = str(product_name).lower()
        
        # Coffee-related keywords
        coffee_keywords = [
            'kopi', 'coffee', 'arabica', 'robusta', 'espresso', 'beans', 'biji',
            'ethiopia', 'gayo', 'toraja', 'aceh', 'sumatra', 'java', 'bali',
            'colombia', 'brazil', 'kenya', 'flores', 'washed', 'natural', 'honey',
            'blend', 'single origin', 'roast', 'gram', 'kg', 'filter', 'drip',
            'anaerobic', 'carbonic', 'maceration', 'wet hulled', 'decaf'
        ]
        
        # Non-coffee keywords
        non_coffee_keywords = [
            'kaos', 'tshirt', 't-shirt', 'shirt', 'baju', 'apparel', 'jersey',
            'hoodie', 'jacket', 'denim', 'topi', 'cap', 'hat',
            'tas', 'bag', 'tote', 'sling', 'backpack',
            'dompet', 'wallet', 'pouch',
            'gelas', 'cup', 'mug', 'tumbler', 'glass', 'server', 'dripper',
            'coaster', 'tatakan',
            'sticker', 'stiker', 'keychain', 'gantungan',
            'tools', 'brewing', 'origami', 'filter paper', 'holder',
            'gift card', 'voucher', 'merchandise'
        ]
        
        # Check non-coffee keywords first
        for keyword in non_coffee_keywords:
            if keyword in name_lower:
                return False
        
        # Check coffee keywords
        for keyword in coffee_keywords:
            if keyword in name_lower:
                return True
        
        return True
    
    def standardize_price(self):

        # Standardize price format
        logger.info("Standardizing price format...")
        
        self.df_raw['price_clean'] = self.df_raw['price'].apply(self.clean_price)
        
        failed = self.df_raw[self.df_raw['price_clean'].isna() & self.df_raw['price'].notna()]
        if len(failed) > 0:
            logger.warning(f"{len(failed)} prices failed to convert")
        else:
            logger.info("All prices converted successfully")
        
        self.stats['price_conversion_failures'] = len(failed)
        return self
    
    def clean_text_fields(self):

        # Clean all text fields
        logger.info("Cleaning text fields...")
        
        text_columns = ['source', 'name', 'description']
        for col in text_columns:
            if col in self.df_raw.columns:
                self.df_raw[f'{col}_clean'] = self.df_raw[col].apply(self.clean_text)
        
        logger.info(" Text fields cleaned")
        return self
    
    def classify_products(self):

        # Classify products as coffee or non-coffee
        logger.info("Classifying products (Coffee vs Non-Coffee)...")
        
        self.df_raw['is_coffee'] = self.df_raw['name'].apply(self.is_coffee_product)
        
        coffee_count = self.df_raw['is_coffee'].sum()
        non_coffee_count = (~self.df_raw['is_coffee']).sum()
        
        logger.info(f" Coffee products: {coffee_count} ({coffee_count/len(self.df_raw)*100:.2f}%)")
        logger.info(f" Non-coffee products: {non_coffee_count} ({non_coffee_count/len(self.df_raw)*100:.2f}%)")
        
        self.stats['coffee_products'] = coffee_count
        self.stats['non_coffee_products'] = non_coffee_count
        return self
    
    def detect_outliers(self):

        # Detect price outliers using IQR method
        logger.info("Detecting price outliers...")
        
        Q1 = self.df_raw['price_clean'].quantile(0.25)
        Q3 = self.df_raw['price_clean'].quantile(0.75)
        IQR = Q3 - Q1
        
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        
        self.df_raw['is_price_outlier'] = (
            (self.df_raw['price_clean'] < lower_bound) | 
            (self.df_raw['price_clean'] > upper_bound)
        )
        
        outliers = self.df_raw['is_price_outlier'].sum()
        logger.info(f" Detected {outliers} price outliers ({outliers/len(self.df_raw)*100:.2f}%)")
        
        self.stats['price_outliers'] = outliers
        self.stats['price_q1'] = Q1
        self.stats['price_median'] = self.df_raw['price_clean'].median()
        self.stats['price_q3'] = Q3
        return self
    
    def flag_missing_critical(self):

        # Flag rows with missing critical fields
        logger.info("Flagging missing critical fields...")
        
        self.df_raw['has_missing_critical'] = (
            self.df_raw['name_clean'].isna() | 
            self.df_raw['price_clean'].isna()
        )
        
        missing = self.df_raw['has_missing_critical'].sum()
        if missing > 0:
            logger.warning(f"{missing} rows with missing critical fields")
        else:
            logger.info(" No missing critical fields")
        
        self.stats['missing_critical'] = missing
        return self
    
    def validate_data_types(self):

        # Validate and set proper data types
        logger.info("Validating data types...")
        
        self.df_raw['price_clean'] = pd.to_numeric(self.df_raw['price_clean'], errors='coerce')
        
        for col in ['source_clean', 'name_clean', 'description_clean']:
            if col in self.df_raw.columns:
                self.df_raw[col] = self.df_raw[col].astype('string')
        
        logger.info(" Data types validated")
        return self
    
    def create_final_dataset(self):

        # Create final cleaned dataset
        logger.info("Creating final dataset...")
        
        self.df_clean = self.df_raw[[
            'source_clean',
            'name_clean',
            'price_clean',
            'description_clean',
            'has_missing_critical',
            'is_price_outlier',
            'is_coffee'
        ]].copy()
        
        self.df_clean.columns = [
            'source',
            'name',
            'price',
            'description',
            'has_missing_critical',
            'is_price_outlier',
            'is_coffee'
        ]
        
        logger.info(f" Final dataset created: {self.df_clean.shape}")
        self.stats['final_rows'] = len(self.df_clean)
        return self
    
    def export_data(self):

        # Export cleaned data to CSV
        logger.info(f"Exporting cleaned data to {self.output_file}...")
        
        try:
            self.df_clean.to_csv(self.output_file, index=False, encoding='utf-8')
            logger.info(f" Data exported successfully")
            
            # Also export quality summary
            summary_file = self.output_file.replace('.csv', '_quality_summary.csv')
            quality_summary = pd.DataFrame({
                'Metric': list(self.stats.keys()),
                'Value': list(self.stats.values())
            })
            quality_summary.to_csv(summary_file, index=False)
            logger.info(f" Quality summary exported to {summary_file}")
            
        except Exception as e:
            logger.error(f"Failed to export data: {e}")
            raise
        
        return self
    
    def generate_report(self):

        # Generate summary report for Data Analytics and Data Science Teams
        logger.info("\n" + "="*80)
        logger.info("DATA CLEANING PIPELINE SUMMARY")
        logger.info("="*80)
        logger.info(f"Original rows: {self.stats.get('original_rows', 0):,}")
        logger.info(f"Final rows: {self.stats.get('final_rows', 0):,}")
        logger.info(f"Duplicates removed: {self.stats.get('duplicates_removed', 0):,}")
        logger.info(f"Coffee products: {self.stats.get('coffee_products', 0):,}")
        logger.info(f"Non-coffee products: {self.stats.get('non_coffee_products', 0):,}")
        logger.info(f"Price outliers: {self.stats.get('price_outliers', 0):,}")
        logger.info(f"Missing critical fields: {self.stats.get('missing_critical', 0):,}")
        logger.info("="*80)
        logger.info(" Pipeline completed successfully!")
        return self
    
    def run(self):

        # Run complete pipeline
        logger.info("Starting data cleaning pipeline...")
        
        try:
            (self
             .load_data()
             .remove_duplicates()
             .standardize_price()
             .clean_text_fields()
             .classify_products()
             .detect_outliers()
             .flag_missing_critical()
             .validate_data_types()
             .create_final_dataset()
             .export_data()
             .generate_report())
            
            return True
        
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            return False


def main():

    # Main entry point
    parser = argparse.ArgumentParser(description='Coffee Data Cleaning Pipeline')
    parser.add_argument('--input', '-i', 
                       default='tokopedia_products_final.csv',
                       help='Input CSV file path')
    parser.add_argument('--output', '-o',
                       default='tokopedia_products_cleaned.csv',
                       help='Output CSV file path')
    
    args = parser.parse_args()
    
    # Run pipeline
    pipeline = CoffeeDataPipeline(args.input, args.output)
    success = pipeline.run()
    
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
