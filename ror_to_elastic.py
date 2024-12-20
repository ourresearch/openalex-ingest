import os
import logging
from datetime import datetime
from elasticsearch import Elasticsearch, NotFoundError
import pandas as pd

ELASTIC_URL = os.getenv("ELASTIC_URL")
es = Elasticsearch([ELASTIC_URL])
logging.basicConfig(level=logging.INFO)

def get_current_ror_file():
    logging.info("Reading current ROR file")
    path = "s3://openalex-ingest/ror/current/ror_snapshot.parquet"
    df = pd.read_parquet(path)
    return df


def format_names(name_records):
    names = []
    primary = None

    for record in name_records:
        name_value = record['value']
        if 'ror_display' in record['types']:
            primary = name_value
        if 'acronym' not in record['types']:
            names.append({"name": [name_value]})

    return names, primary


def extract_relationships(rel_records):
    if pd.isna(rel_records).any() or len(rel_records) == 0:
        return [], []

    ids = []
    types = []
    for record in rel_records:
        ids.append(record['id'])
        types.append(record['type'])

    return ids, types


def get_country(location_str):
    if pd.isna(location_str).any() or len(location_str) == 0:
        return None

    return location_str[0]['geonames_details']['country_code']


def get_dates(admin_dict):
    created = admin_dict.get('created', {}).get('date')
    updated = admin_dict.get('last_modified', {}).get('date')
    return created, updated


def transform_record(row):
    names, primary = format_names(row['names'])
    rel_ids, rel_types = extract_relationships(row['relationships'])
    created_date, updated_date = get_dates(row['admin'])

    return {
        "_id": row['id'],
        "_index": "search-ror-institutions-v2",
        "_score": 0,
        "country": get_country(row['locations']),
        "id": row['id'],
        "names": names,
        "primary": primary,
        "relationships.id": rel_ids,
        "relationships.type": rel_types,
        "status": row['status'],
        "created_date": created_date,
        "updated_date": updated_date
    }


def get_existing_record(client, record_id, index):
    try:
        response = client.get(index=index, id=record_id)
        return response['_source']
    except NotFoundError:
        return None


def should_update_record(existing_record, new_record):
    """Determine if record should be updated"""
    if not existing_record:
        return True

    try:
        # Check if updated_date exists in both records
        existing_date = existing_record.get('updated_date')
        new_date = new_record.get('updated_date')

        # If either date is missing, assume we should update
        if not existing_date:
            return True

        # Convert date strings to datetime objects for comparison
        existing_date = datetime.strptime(existing_date, '%Y-%m-%d')
        new_date = datetime.strptime(new_date, '%Y-%m-%d')

        return new_date > existing_date

    except (ValueError, KeyError, TypeError) as e:
        logging.warning(f"Error comparing dates, defaulting to update record: {str(e)}")
        return True


def save_to_elasticsearch(client, record):
    existing_record = get_existing_record(client, record['_id'], record['_index'])
    if should_update_record(existing_record, record):
        logging.info(f"Saving/updating record {record['_id']} to Elasticsearch")
        response = client.index(
            index=record['_index'],
            id=record['_id'],
            document={
                "country": record['country'],
                "id": record['id'],
                "names": record['names'],
                "primary": record['primary'],
                "relationships.id": record['relationships.id'],
                "relationships.type": record['relationships.type'],
                "status": record['status'],
                "created_date": record['created_date'],
                "updated_date": record['updated_date']
            }
        )
        return response
    else:
        logging.info(f"Skipping record {record['_id']} - no update needed")
        return None


if __name__ == "__main__":
    df = get_current_ror_file()

    updated_count = 0
    total_count = 0
    for index, row in df.iterrows():
        total_count += 1
        record = transform_record(row)
        response = save_to_elasticsearch(es, record)
        logging.info(f"Count processed is {total_count}")
        if response:
            updated_count += 1

    logging.info(f"Processed {total_count} records")
    logging.info(f"Updated {updated_count} records in Elasticsearch")
