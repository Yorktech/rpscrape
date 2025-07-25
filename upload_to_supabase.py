#!/usr/bin/env python3
"""
Script to upload racing results CSV to Supabase database.
Handles complex CSV parsing with comments containing special characters.
"""

import pandas as pd
import csv
from supabase import create_client, Client
from datetime import datetime
import logging
import sys
import os
import glob
import shutil
from typing import Dict, Any, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Supabase configuration
SUPABASE_URL = "https://dxdkmokqeweqfwknyhla.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImR4ZGttb2txZXdlcWZ3a255aGxhIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDkzMjkwOTMsImV4cCI6MjA2NDkwNTA5M30.JoZyNvjrvM7QV9g-ubebHuCtcDIxjVsH2gbxZZ666lY"

def safe_convert_to_int(value: Any) -> Optional[int]:
    """Safely convert a value to integer, returning None if conversion fails."""
    if pd.isna(value) or value == '' or value is None:
        return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None

def safe_convert_to_float(value: Any) -> Optional[float]:
    """Safely convert a value to float, returning None if conversion fails."""
    if pd.isna(value) or value == '' or value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def safe_convert_to_str(value: Any) -> Optional[str]:
    """Safely convert a value to string, returning None for empty/null values."""
    if pd.isna(value) or value == '' or value is None:
        return None
    return str(value).strip()

def parse_csv_file(file_path: str) -> list:
    """
    Parse the CSV file with proper handling of complex comment fields.
    Uses csv.reader with custom quoting to handle the racing comments properly.
    """
    logger.info(f"Starting to parse CSV file: {file_path}")
    
    rows = []
    
    # Define expected column names
    expected_columns = [
        'date', 'region', 'course', 'off', 'race_name', 'type', 'class', 'pattern', 
        'rating_band', 'age_band', 'sex_rest', 'dist', 'dist_f', 'dist_m', 'going', 
        'ran', 'num', 'pos', 'draw', 'ovr_btn', 'btn', 'horse', 'age', 'sex', 'lbs', 
        'hg', 'time', 'secs', 'dec', 'jockey', 'trainer', 'prize', 'or', 'rpr', 
        'sire', 'dam', 'damsire', 'owner', 'comment'
    ]
    
    try:
        with open(file_path, 'r', encoding='utf-8', newline='') as csvfile:
            # Use csv.reader with flexible quoting
            reader = csv.reader(csvfile, quotechar='"', quoting=csv.QUOTE_MINIMAL)
            
            # Read header
            header = next(reader)
            logger.info(f"CSV header: {header}")
            
            if len(header) != len(expected_columns):
                logger.warning(f"Column count mismatch. Expected {len(expected_columns)}, got {len(header)}")
            
            row_count = 0
            for row_num, row in enumerate(reader, start=2):  # Start at 2 since header is row 1
                try:
                    # Handle rows with different column counts
                    if len(row) != len(expected_columns):
                        # If we have fewer columns, pad with empty strings
                        while len(row) < len(expected_columns):
                            row.append('')
                        # If we have more columns, it's likely the comment field was split
                        # Rejoin the excess columns as part of the comment
                        if len(row) > len(expected_columns):
                            comment_parts = row[38:]  # Everything from comment column onwards
                            row = row[:38] + [','.join(comment_parts)]
                    
                    # Create dictionary with proper column mapping
                    row_dict = {}
                    for i, col_name in enumerate(expected_columns):
                        value = row[i] if i < len(row) else ''
                        row_dict[col_name] = value
                    
                    rows.append(row_dict)
                    row_count += 1
                    
                    if row_count % 100 == 0:
                        logger.info(f"Processed {row_count} rows...")
                        
                except Exception as e:
                    logger.error(f"Error processing row {row_num}: {e}")
                    logger.error(f"Row data: {row}")
                    continue
                    
        logger.info(f"Successfully parsed {row_count} rows from CSV")
        return rows
        
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}")
        raise

def transform_row_for_supabase(row: Dict[str, Any]) -> Dict[str, Any]:
    """Transform a CSV row into a format suitable for Supabase insertion."""
    
    # Handle the 'or' column name conflict (reserved keyword)
    transformed = {
        'date': safe_convert_to_str(row.get('date')),
        'region': safe_convert_to_str(row.get('region')),
        'course': safe_convert_to_str(row.get('course')),
        'off': safe_convert_to_str(row.get('off')),
        'race_name': safe_convert_to_str(row.get('race_name')),
        'type': safe_convert_to_str(row.get('type')),
        'class': safe_convert_to_str(row.get('class')),
        'pattern': safe_convert_to_str(row.get('pattern')),
        'rating_band': safe_convert_to_str(row.get('rating_band')),
        'age_band': safe_convert_to_str(row.get('age_band')),
        'sex_rest': safe_convert_to_str(row.get('sex_rest')),
        'dist': safe_convert_to_str(row.get('dist')),
        'dist_f': safe_convert_to_str(row.get('dist_f')),
        'dist_m': safe_convert_to_int(row.get('dist_m')),
        'going': safe_convert_to_str(row.get('going')),
        'ran': safe_convert_to_int(row.get('ran')),
        'num': safe_convert_to_int(row.get('num')),
        'pos': safe_convert_to_int(row.get('pos')),
        'draw': safe_convert_to_int(row.get('draw')),
        'ovr_btn': safe_convert_to_float(row.get('ovr_btn')),
        'btn': safe_convert_to_float(row.get('btn')),
        'horse': safe_convert_to_str(row.get('horse')),
        'age': safe_convert_to_int(row.get('age')),
        'sex': safe_convert_to_str(row.get('sex')),
        'lbs': safe_convert_to_int(row.get('lbs')),
        'hg': safe_convert_to_str(row.get('hg')),
        'time': safe_convert_to_str(row.get('time')),
        'secs': safe_convert_to_float(row.get('secs')),
        'dec': safe_convert_to_float(row.get('dec')),
        'jockey': safe_convert_to_str(row.get('jockey')),
        'trainer': safe_convert_to_str(row.get('trainer')),
        'prize': safe_convert_to_float(row.get('prize')),
        'or_rating': safe_convert_to_int(row.get('or')),  # Note: 'or' -> 'or_rating'
        'rpr': safe_convert_to_int(row.get('rpr')),
        'sire': safe_convert_to_str(row.get('sire')),
        'dam': safe_convert_to_str(row.get('dam')),
        'damsire': safe_convert_to_str(row.get('damsire')),
        'owner': safe_convert_to_str(row.get('owner')),
        'comment': safe_convert_to_str(row.get('comment'))
    }
    
    return transformed

def upload_to_supabase(rows: list, batch_size: int = 100) -> bool:
    """Upload rows to Supabase in batches."""
    logger.info("Initializing Supabase client...")
    
    try:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        logger.info("Supabase client initialized successfully")
        
        total_rows = len(rows)
        logger.info(f"Starting upload of {total_rows} rows in batches of {batch_size}")
        
        successful_uploads = 0
        failed_uploads = 0
        
        for i in range(0, total_rows, batch_size):
            batch = rows[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            
            logger.info(f"Processing batch {batch_num} ({len(batch)} rows)...")
            
            # Transform each row in the batch
            transformed_batch = []
            for row in batch:
                try:
                    transformed_row = transform_row_for_supabase(row)
                    transformed_batch.append(transformed_row)
                except Exception as e:
                    logger.error(f"Error transforming row: {e}")
                    logger.error(f"Row data: {row}")
                    failed_uploads += 1
                    continue
            
            if not transformed_batch:
                logger.warning(f"Batch {batch_num} is empty after transformation, skipping...")
                continue
            
            try:
                # Insert batch into Supabase with UPSERT to handle duplicates
                # Use date, course, race_name, horse, pos as unique key
                result = supabase.table('historical_racing_results').upsert(
                    transformed_batch,
                    on_conflict='date,course,race_name,horse,pos'
                ).execute()
                
                if result.data:
                    successful_uploads += len(result.data)
                    logger.info(f"Batch {batch_num} uploaded successfully ({len(result.data)} rows)")
                else:
                    logger.error(f"Batch {batch_num} upload failed - no data returned")
                    failed_uploads += len(batch)
                    
            except Exception as e:
                logger.error(f"Error uploading batch {batch_num}: {e}")
                failed_uploads += len(batch)
                continue
        
        logger.info(f"Upload completed. Successful: {successful_uploads}, Failed: {failed_uploads}")
        return failed_uploads == 0
        
    except Exception as e:
        logger.error(f"Error initializing Supabase or during upload: {e}")
        return False

def process_all_csv_files(unprocessed_folder: str, processed_folder: str) -> bool:
    """Process all CSV files in the unprocessed folder."""
    logger.info(f"Looking for CSV files in: {unprocessed_folder}")
    
    # Find all CSV files in the unprocessed folder
    csv_pattern = os.path.join(unprocessed_folder, "*.csv")
    csv_files = glob.glob(csv_pattern)
    
    if not csv_files:
        logger.warning(f"No CSV files found in {unprocessed_folder}")
        return True
    
    logger.info(f"Found {len(csv_files)} CSV files to process")
    
    # Ensure processed folder exists
    os.makedirs(processed_folder, exist_ok=True)
    
    total_successful = 0
    total_failed = 0
    
    for csv_file_path in csv_files:
        filename = os.path.basename(csv_file_path)
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing file: {filename}")
        logger.info(f"{'='*60}")
        
        try:
            # Parse CSV file
            rows = parse_csv_file(csv_file_path)
            
            if not rows:
                logger.error(f"No rows found in {filename}")
                total_failed += 1
                continue
            
            logger.info(f"Parsed {len(rows)} rows from {filename}")
            
            # Upload to Supabase
            success = upload_to_supabase(rows)
            
            if success:
                logger.info(f"✅ Upload completed successfully for {filename}")
                
                # Move file to processed folder
                processed_file_path = os.path.join(processed_folder, filename)
                shutil.move(csv_file_path, processed_file_path)
                logger.info(f"📁 Moved {filename} to processed folder")
                
                total_successful += 1
            else:
                logger.error(f"❌ Upload failed for {filename}")
                total_failed += 1
                
        except Exception as e:
            logger.error(f"❌ Fatal error processing {filename}: {e}")
            total_failed += 1
            continue
    
    logger.info(f"\n{'='*60}")
    logger.info(f"BATCH PROCESSING COMPLETE")
    logger.info(f"{'='*60}")
    logger.info(f"✅ Successfully processed: {total_successful} files")
    logger.info(f"❌ Failed to process: {total_failed} files")
    
    return total_failed == 0

def main():
    """Main function to orchestrate the CSV upload process."""
    # Define folder paths
    base_path = r"d:\Source\Repos\rpscrape\data"
    unprocessed_folder = os.path.join(base_path, "unprocessed")
    processed_folder = os.path.join(base_path, "processed")
    
    logger.info("Starting batch CSV to Supabase upload process...")
    logger.info(f"Unprocessed folder: {unprocessed_folder}")
    logger.info(f"Processed folder: {processed_folder}")
    
    try:
        # Process all CSV files in the unprocessed folder
        success = process_all_csv_files(unprocessed_folder, processed_folder)
        
        if success:
            logger.info("🎉 All files processed successfully!")
            sys.exit(0)
        else:
            logger.error("⚠️ Some files failed to process")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
