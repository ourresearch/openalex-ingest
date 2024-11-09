import os
from urllib.parse import urlparse

import logging
import sqlalchemy
from sqlalchemy.orm import declarative_base, sessionmaker

S3_BUCKET = 'openalex-ingest'

def _make_logger():
    logger = logging.getLogger('openalex-ingest')
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

LOGGER = _make_logger()


def get_database_url():
    """Convert postgres:// to postgresql:// if necessary"""
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is not set")

    parsed = urlparse(database_url)
    if parsed.scheme == 'postgres':
        return database_url.replace('postgres:', 'postgresql:', 1)
    return database_url

engine = sqlalchemy.create_engine(get_database_url())
Base = declarative_base()
Session = sessionmaker(bind=engine)
db = Session()
