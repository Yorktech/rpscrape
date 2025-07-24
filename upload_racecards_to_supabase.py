#!/usr/bin/env python3
"""
Upload racecards JSON data to Supabase
"""

import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from supabase import create_client, Client

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('racecards_upload.log')
    ]
)

# Load environment variables
load_dotenv()

def get_supabase_client() -> Client:
    """Initialize and return Supabase client"""
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    
    if not url or not key:
        raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in environment")
    
    return create_client(url, key)

def safe_int(value):
    """Safely convert value to int, return None if conversion fails"""
    if value is None or value == '':
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None

def safe_float(value):
    """Safely convert value to float, return None if conversion fails"""
    if value is None or value == '':
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def safe_date(date_str):
    """Safely convert date string to date format"""
    if not date_str:
        return None
    try:
        # Handle different date formats
        if 'T' in date_str:
            date_str = date_str.split('T')[0]
        return date_str
    except:
        return None

def flatten_race_data(races_data):
    """Flatten nested race structure into individual rows"""
    rows = []
    
    for region, courses in races_data.items():
        for course, times in courses.items():
            for off_time, race_data in times.items():
                # Extract race-level data
                race_info = {
                    'race_id': race_data.get('race_id'),
                    'date': safe_date(race_data.get('date')),
                    'course': race_data.get('course'),
                    'course_id': safe_int(race_data.get('course_id')),
                    'region': race_data.get('region'),
                    'off_time': race_data.get('off_time'),
                    'race_name': race_data.get('race_name'),
                    'distance_round': race_data.get('distance_round'),
                    'distance': race_data.get('distance'),
                    'distance_f': safe_float(race_data.get('distance_f')),
                    'pattern': race_data.get('pattern') or '',
                    'race_class': race_data.get('race_class') or '',
                    'type': race_data.get('type'),
                    'age_band': race_data.get('age_band'),
                    'rating_band': race_data.get('rating_band'),
                    'prize': race_data.get('prize'),
                    'field_size': safe_int(race_data.get('field_size')),
                    'going': race_data.get('going'),
                    'going_detailed': race_data.get('going_detailed'),
                    'rail_movements': json.dumps(race_data.get('rail_movements')) if race_data.get('rail_movements') else None,
                    'stalls': race_data.get('stalls'),
                    'weather': race_data.get('weather'),
                    'surface': race_data.get('surface')
                }
                
                # Process each runner
                for runner in race_data.get('runners', []):
                    row = race_info.copy()
                    
                    # Add horse information
                    row.update({
                        'horse_id': safe_int(runner.get('horse_id')),
                        'horse_name': runner.get('name'),
                        'number': safe_int(runner.get('number')),
                        'draw': safe_int(runner.get('draw')),
                        'age': safe_int(runner.get('age')),
                        'sex': runner.get('sex'),
                        'sex_code': runner.get('sex_code'),
                        'colour': runner.get('colour'),
                        'horse_region': runner.get('region'),
                        'dob': safe_date(runner.get('dob')),
                        
                        # Breeding information
                        'breeder': runner.get('breeder'),
                        'sire': runner.get('sire'),
                        'sire_region': runner.get('sire_region'),
                        'dam': runner.get('dam'),
                        'dam_region': runner.get('dam_region'),
                        'grandsire': runner.get('grandsire'),
                        'damsire': runner.get('damsire'),
                        'damsire_region': runner.get('damsire_region'),
                        
                        # Connections
                        'trainer': runner.get('trainer'),
                        'trainer_id': safe_int(runner.get('trainer_id')),
                        'trainer_location': runner.get('trainer_location'),
                        'trainer_14_days': json.dumps(runner.get('trainer_14_days')) if runner.get('trainer_14_days') else None,
                        'trainer_rtf': runner.get('trainer_rtf'),
                        'owner': runner.get('owner'),
                        'jockey': runner.get('jockey'),
                        'jockey_id': safe_int(runner.get('jockey_id')),
                        
                        # Racing data
                        'lbs': safe_int(runner.get('lbs')),
                        'ofr': safe_int(runner.get('ofr')),
                        'rpr': safe_int(runner.get('rpr')),
                        'ts': safe_int(runner.get('ts')),
                        'headgear': runner.get('headgear'),
                        'headgear_first': runner.get('headgear_first'),
                        'last_run': runner.get('last_run'),
                        'form': runner.get('form'),
                        
                        # Previous connections (as JSON)
                        'prev_trainers': json.dumps(runner.get('prev_trainers')) if runner.get('prev_trainers') else None,
                        'prev_owners': json.dumps(runner.get('prev_owners')) if runner.get('prev_owners') else None,
                        
                        # Comments
                        'comment': runner.get('comment'),
                        'spotlight': runner.get('spotlight'),
                        
                        # Medical and quotes (as JSON)
                        'medical': json.dumps(runner.get('medical')) if runner.get('medical') else None,
                        'quotes': json.dumps(runner.get('quotes')) if runner.get('quotes') else None,
                        'stable_tour': json.dumps(runner.get('stable_tour')) if runner.get('stable_tour') else None,
                        
                        # Statistics (as JSON)
                        'stats': json.dumps(runner.get('stats')) if runner.get('stats') else None
                    })
                    
                    rows.append(row)
    
    return rows

def upload_to_supabase(data, supabase_client):
    """Upload data to Supabase in batches"""
    if not data:
        logging.warning("No data to upload")
        return
    
    batch_size = 100
    total_rows = len(data)
    successful_uploads = 0
    failed_uploads = 0
    
    logging.info(f"Starting upload of {total_rows} rows in batches of {batch_size}")
    
    for i in range(0, total_rows, batch_size):
        batch = data[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        
        try:
            logging.info(f"Processing batch {batch_num} ({len(batch)} rows)...")
            
            response = supabase_client.table('racecards').insert(batch).execute()
            
            if response.data:
                successful_uploads += len(batch)
                logging.info(f"Batch {batch_num} uploaded successfully ({len(batch)} rows)")
            else:
                failed_uploads += len(batch)
                logging.error(f"Batch {batch_num} failed: No data returned")
                
        except Exception as e:
            failed_uploads += len(batch)
            logging.error(f"Batch {batch_num} failed: {str(e)}")
            continue
    
    logging.info(f"Upload completed. Successful: {successful_uploads}, Failed: {failed_uploads}")
    return successful_uploads, failed_uploads

def process_json_file(file_path, supabase_client):
    """Process a single JSON file and upload to Supabase"""
    logging.info(f"Processing file: {file_path}")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            races_data = json.load(f)
        
        if not races_data:
            logging.warning(f"File {file_path} contains no data")
            return 0, 0
        
        # Flatten the data structure
        flattened_data = flatten_race_data(races_data)
        
        if not flattened_data:
            logging.warning(f"No rows extracted from {file_path}")
            return 0, 0
        
        logging.info(f"Extracted {len(flattened_data)} rows from {file_path}")
        
        # Upload to Supabase
        successful, failed = upload_to_supabase(flattened_data, supabase_client)
        
        return successful, failed
        
    except Exception as e:
        logging.error(f"Error processing {file_path}: {str(e)}")
        return 0, 0

def main():
    """Main function"""
    if len(sys.argv) < 2:
        print("Usage: python upload_racecards_to_supabase.py <json_file_or_directory>")
        sys.exit(1)
    
    target_path = sys.argv[1]
    
    try:
        # Initialize Supabase client
        logging.info("Initializing Supabase client...")
        supabase = get_supabase_client()
        logging.info("Supabase client initialized successfully")
        
        total_successful = 0
        total_failed = 0
        
        if os.path.isfile(target_path):
            # Process single file
            successful, failed = process_json_file(target_path, supabase)
            total_successful += successful
            total_failed += failed
        
        elif os.path.isdir(target_path):
            # Process all JSON files in directory
            json_files = list(Path(target_path).glob("*.json"))
            
            if not json_files:
                logging.warning(f"No JSON files found in {target_path}")
                return
            
            logging.info(f"Found {len(json_files)} JSON files to process")
            
            for json_file in json_files:
                successful, failed = process_json_file(json_file, supabase)
                total_successful += successful
                total_failed += failed
        
        else:
            logging.error(f"Path {target_path} is neither a file nor a directory")
            sys.exit(1)
        
        logging.info("="*60)
        logging.info("UPLOAD SUMMARY")
        logging.info("="*60)
        logging.info(f"Total successful uploads: {total_successful}")
        logging.info(f"Total failed uploads: {total_failed}")
        
        if total_failed == 0:
            logging.info("🎉 All uploads completed successfully!")
        else:
            logging.warning(f"⚠️ {total_failed} uploads failed")
        
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
