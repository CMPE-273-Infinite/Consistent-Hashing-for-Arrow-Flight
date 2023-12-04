from dotenv import load_dotenv

load_dotenv()
import logging
import boto3
from botocore.exceptions import ClientError
import os

def create_bucket(bucket_name,region=None):
    try:
        print('created bucket')
        if(region is None):
            s3_client = boto3.client('s3')
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client = boto3.client('s3',region_name=region)
            location = {'LocationConstraint':region}
            s3_client.create_bucket(Bucket=bucket_name,CreateBucketConfiguration=location)
        
    except ClientError as e
            logging.error(e)
            return false
    
return true
    
def upload_file(file_name, bucket, object_name=None):
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def download_file(bucket_name,object_name,object_name):
    
