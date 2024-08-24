import boto3
import pandas as pd
import glob
from tqdm import tqdm
import tarfile
import os.path
import os 
from dotenv import load_dotenv
load_dotenv()

s3 = boto3.resource(
    service_name='s3',
    region_name='ap-southeast-1',
    aws_access_key_id=os.getenv('S3_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('S3_SECRET_ACCESS_KEY')
)
BUCKET = 'gzawsbucket'

# two options: zip dir or not loop over files

# zip dir
def make_tarfile(output_filename, source_dir):
    with tarfile.open(output_filename, "w:gz") as tar:
        tar.add(source_dir, arcname=os.path.basename(source_dir))

if False:
    source_dir = 'data/processed/pre/'
    destination_dir = 'data/processed/pre_v1.tar.gz'
    make_tarfile(output_filename=destination_dir, source_dir=source_dir)
    print(f'finished making the tar on {source_dir}, the destination file is {destination_dir} !')

    s3.Bucket(BUCKET).upload_file(destination_dir, destination_dir)
    print(f"finished uploading file {destination_dir} !")



if True:
    # local
    file_path = 'data/et/all_transcripts.parquet'

    s3.Bucket(BUCKET).upload_file(file_path, file_path)
