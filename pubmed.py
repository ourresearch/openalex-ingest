import gzip
import tempfile
from ftplib import FTP

import boto3

from common import S3_BUCKET, LOGGER


# FTP file functions
def pubmed_ftp_client():
    ftp = FTP('ftp.ncbi.nlm.nih.gov')
    ftp.login()
    ftp.cwd('/pubmed/updatefiles/')
    return ftp

def retrieve_file(ftp_client, filename):
    local_filename = tempfile.mkstemp()[1]

    with open(local_filename, 'wb') as f:
        ftp_client.retrbinary(f'RETR {filename}', f.write)

    return local_filename

def get_xml_content(xml_gz_filename):
    with gzip.open(xml_gz_filename, "rb") as xml_file:
        return xml_file.read()

def get_existing_fnames(s3):
    existing_fnames = set()
    obj_list = s3.list_objects_v2(Bucket=S3_BUCKET,
                                   Prefix='pubmed',
                                   MaxKeys=1000)
    for o in obj_list["Contents"]:
        existing_fnames.add(o['Key'])
    if 'NextToken' not in obj_list or not obj_list['NextToken']:
        return existing_fnames
    while True:
        obj_list = s3.list_objects_v2(Bucket=S3_BUCKET,
                                   Prefix='pubmed',
                                   MaxKeys=1000,
                                   ContinuationToken=obj_list['NextToken'])
        for o in obj_list["Contents"]:
            existing_fnames.add(o['Key'])
        if 'NextToken' not in obj_list or not obj_list['NextToken']:
            break
    return existing_fnames

def main():
    s3 = boto3.client('s3')
    existing_fnames = get_existing_fnames(s3)
    ftp = pubmed_ftp_client()
    remote_filenames = sorted([f for f in ftp.nlst() if f.endswith('.xml.gz')])
    ftp.quit()
    for i, filename in enumerate([f for f in remote_filenames if f not in existing_fnames]):
        LOGGER.info(f'Fetching {filename} ({i + 1}/{len(remote_filenames)})')
        ftp = pubmed_ftp_client()
        temp_fname = retrieve_file(ftp, filename)
        s3.upload_file(temp_fname, S3_BUCKET, 'pubmed/' + filename)
        ftp.quit()
        LOGGER.info(f'Finished {filename} ({i + 1}/{len(remote_filenames)})')

if __name__ == '__main__':
    main()