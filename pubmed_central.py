import os
import tarfile
import tempfile
from ftplib import FTP
import boto3
from pathlib import Path

S3_BUCKET = "openalex-ingest"
PMC_SUBDIRS = [
    "oa_bulk/oa_comm/xml",
    "oa_bulk/oa_noncomm/xml",
    "oa_bulk/oa_other/xml",
    "manuscript/xml"
]

s3_client = boto3.client("s3")

def pubmed_ftp_client():
    ftp = FTP("ftp.ncbi.nlm.nih.gov")
    ftp.login()
    return ftp

def list_tarballs():
    ftp = pubmed_ftp_client()
    tarballs = []

    for subdir in PMC_SUBDIRS:
        full_path = f"/pub/pmc/{subdir}"
        ftp.cwd(full_path)
        for fname in ftp.nlst():
            if fname.endswith(".tar.gz"):
                tarballs.append((f"{full_path}/{fname}", subdir, fname))

    ftp.quit()
    return tarballs

def download_tarball(ftp_path, local_path):
    ftp = pubmed_ftp_client()
    with open(local_path, "wb") as f:
        ftp.retrbinary(f"RETR {ftp_path}", f.write)
    ftp.quit()

def extract_and_upload(local_tarball, s3_prefix):
    with tarfile.open(local_tarball, "r:gz") as tar:
        for member in tar.getmembers():
            if member.isfile() and member.name.endswith(('.nxml', '.xml')):
                extracted = tar.extractfile(member)
                if extracted is None:
                    continue

                s3_key = f"{s3_prefix}/{member.name}"
                s3_client.upload_fileobj(extracted, S3_BUCKET, s3_key)
                print(f"Uploaded to s3://{S3_BUCKET}/{s3_key}")

def already_processed(s3_prefix):
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=s3_prefix, MaxKeys=1)
    return "Contents" in response

def main():
    tarballs = list_tarballs()
    print(f"Found {len(tarballs)} tarballs to process.")

    for ftp_path, subdir, fname in tarballs:
        archive_name = fname.replace('.tar.gz', '')
        s3_prefix = f"pubmed_central/{subdir}/{archive_name}"

        if already_processed(s3_prefix):
            print(f"Skipping {ftp_path} — already processed.")
            continue

        print(f"Processing: {ftp_path}")
        with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as tmp_file:
            tmp_path = tmp_file.name

        try:
            download_tarball(ftp_path, tmp_path)
            extract_and_upload(tmp_path, s3_prefix)
        finally:
            os.remove(tmp_path)

if __name__ == "__main__":
    main()
