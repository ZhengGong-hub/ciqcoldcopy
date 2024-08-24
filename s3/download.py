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

# download file
def download_file(output_filename, source_dir):
    s3.Bucket('gzawsbucket').download_file(Key=source_dir, Filename=output_filename)


download_file(
    output_filename='data/processed/pre_v1.tar.gz',
    source_dir='data/processed/pre_v1.tar.gz'
)