import argparse
import boto3
import os
import pandas as pd
import subprocess
from multiprocessing import Pool
from datetime import datetime

from common import S3_BUCKET, LOGGER

ORCID_ACCESS_KEY = os.getenv('ORCID_ACCESS_KEY')
ORCID_SECRET_KEY = os.getenv('ORCID_SECRET_KEY')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
date_format = '%Y-%m-%d %H:%M:%S.%f'
date_format_no_millis = '%Y-%m-%d %H:%M:%S'
max_threads = 32
summaries_bucket = 'v3.0-summaries'

# set up orcid s3 client
s3_orcid = boto3.client('s3', 
                aws_access_key_id=ORCID_ACCESS_KEY,
                aws_secret_access_key=ORCID_SECRET_KEY)

# set up openalex s3 client
s3_openalex = boto3.client('s3', 
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

def get_last_update_date(s3_openalex, orcid_type='summaries'):
    try:
        last_modified_df = pd.read_parquet(f"s3://openalex-ingest/orcid/last_modified_date_{orcid_type}.parquet")
        last_update_date = last_modified_df['last_modified'].max()
        return last_update_date
    except Exception as e:
        LOGGER.error(f"Error getting last update file: {e}")
        return None
    
def sync_orcid_summary(orcid_to_sync):
    suffix = orcid_to_sync[-3:]
    prefix = f"{suffix}/{orcid_to_sync}.xml"
    file_path = f"./summaries/{suffix}/"
    file_name = f"{orcid_to_sync}.xml"
    os.makedirs(file_path, exist_ok=True)
    
    LOGGER.debug(f"'Downloading {file_name} to {file_path}")

    # set up orcid s3 client
    s3_orcid_temp = boto3.client('s3', 
                    aws_access_key_id=ORCID_ACCESS_KEY,
                    aws_secret_access_key=ORCID_SECRET_KEY)

    # set up openalex s3 client
    s3_openalex_temp = boto3.client('s3', 
                    aws_access_key_id=AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    
    try:
        # Downloading the file
        s3_orcid_temp.download_file(summaries_bucket, prefix, f"{file_path}{file_name}")
        try:
            # Uploading the file
            s3_openalex_temp.upload_file(f"{file_path}{file_name}", 'openalex-ingest', f"orcid/summaries/{prefix}")
            # with open(f'./files_done/{orcid_to_sync}.txt', 'w') as f:
            #     f.write('Y')
            os.remove(f"{file_path}{file_name}")
        except Exception as e:
            LOGGER.exception(f"Error uploading {orcid_to_sync}")
            LOGGER.exception(e)
    except Exception as e:
        LOGGER.exception(f"Error fetching {orcid_to_sync}")
        LOGGER.exception(e)

def main():
    parser = argparse.ArgumentParser(description='Pull ORCID data (summaries or activities)')
    parser.add_argument('mode', choices=['summaries', 'activities'], help='Specify whether to update ORCID summaries or activities.')
    args = parser.parse_args()
    
    # get last date for past updates
    last_update = get_last_update_date(s3_openalex, args.mode)
    if not last_update:
        return
    LOGGER.info(f"Last update date: {last_update}")

    LOGGER.info('Downloading the lambda file')	
    s3_orcid.download_file('orcid-lambda-file', 'last_modified.csv.tar', './last_modified.csv.tar')

    LOGGER.info('Uploading the lambda file to S3')
    s3_openalex.upload_file('last_modified.csv.tar', 'openalex-ingest', 'orcid/last_modified.csv.tar')

    LOGGER.info('Decompressing the lambda file')
    subprocess.call('tar -xzvf last_modified.csv.tar', shell=True)
    os.remove('last_modified.csv.tar')

    records_to_sync = []

    is_first_line = True

    for line in open('last_modified.csv', 'r'):
        if is_first_line:
            is_first_line = False
            continue
        else:
            line = line.rstrip('\n')
            elements = line.split(',')
            last_modified_str = elements[3]
            try:
                latest_date_in_file = datetime.strptime(last_modified_str, date_format)
            except ValueError:
                latest_date_in_file = datetime.strptime(last_modified_str, date_format_no_millis)
            break
    
    is_first_line = True
    
    for line in open('last_modified.csv', 'r'):
        if is_first_line:
            is_first_line = False
            continue

        line = line.rstrip('\n')
        elements = line.split(',')
        orcid = elements[0]
        
        last_modified_str = elements[3]
        try:
            last_modified_date = datetime.strptime(last_modified_str, date_format)
        except ValueError:
            last_modified_date = datetime.strptime(last_modified_str, date_format_no_millis)
        
        if last_modified_date >= last_update:
            records_to_sync.append(orcid)
            if len(records_to_sync) % 100000 == 0:
                LOGGER.info(f"Records to sync so far: {len(records_to_sync)}")
        else:
            # Since the lambda file is ordered by last_modified date descendant, 
            # when last_modified_date < last_sync we don't need to parse any more lines
            break

    ######## TEMP CODE ########
    
    # read in file names for all records that have been processed (file names in files done directory)
    # files_done = set([x.split('.txt')[0] for x in os.listdir('files_done')])

    # LOGGER.info(f"Files already processed: {len(files_done)}")

    # # efficiently remove records that have already been processed
    # records_to_sync = list(set(records_to_sync) - set(files_done))

    # latest_date_in_file = datetime.strptime('2024-10-30 00:00:01.826065', date_format)

    ###########################

    # delete the lambda file
    os.remove('last_modified.csv')

    LOGGER.info(f"Records to sync: {len(records_to_sync)}")
    LOGGER.info(f"Latest date in file: {latest_date_in_file}")

    if len(records_to_sync) == 0:
        LOGGER.info("No records to sync")
        return
    
    if args.mode == 'summaries':
        LOGGER.info("Updating ORCID summaries")
        os.makedirs("summaries", exist_ok=True)

        pool = Pool(processes=max_threads)
        pool.map(sync_orcid_summary, records_to_sync)
        
        pool.close()
        pool.join()

        LOGGER.info('All files are in sync now')

        pd.DataFrame(zip([latest_date_in_file]), columns=['last_modified']) \
            .to_parquet(f"s3://openalex-ingest/orcid/last_modified_date_summaries.parquet")

    elif args.mode == 'activites':
        LOGGER.info("Updating ORCID activities NOT YET SETUP")
        # set this up soon
        pass


if __name__ == '__main__':
    main()
