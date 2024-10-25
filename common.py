import logging

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
