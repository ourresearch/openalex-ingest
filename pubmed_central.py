import os
import tempfile
from ftplib import FTP
import boto3

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

def upload_tarball_to_s3(local_tarball, s3_key):
    s3_client.upload_file(local_tarball, S3_BUCKET, s3_key)
    print(f"Uploaded archive to s3://{S3_BUCKET}/{s3_key}")

def already_uploaded(s3_key):
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=s3_key, MaxKeys=1)
    return "Contents" in response

def main():
    tarballs = list_tarballs()
    print(f"Found {len(tarballs)} tarballs to process.")

    for ftp_path, subdir, fname in tarballs:
        s3_key = f"pubmed_central/{subdir}/{fname}"

        if already_uploaded(s3_key):
            print(f"Skipping {ftp_path} — already uploaded.")
            continue

        print(f"Processing: {ftp_path}")
        with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as tmp_file:
            tmp_path = tmp_file.name

        try:
            download_tarball(ftp_path, tmp_path)
            upload_tarball_to_s3(tmp_path, s3_key)
        finally:
            os.remove(tmp_path)

if __name__ == "__main__":
    main()