# import boto3
# s3 = boto3.client('s3')
#
# with open('filename', 'rb') as data:
#     s3.upload_fileobj(data, 'mybucket', 'mykey')
#
# #--Check file exists
# #---Check Bucket exists
# #---Upload file
#
#
# http://boto3.readthedocs.io/en/latest/reference/services/s3.html#S3.Client.upload_fileobj
#
#
# import boto3
# s3 = boto3.resource('s3')
# s3.meta.client.upload_file('/tmp/hello.txt', 'mybucket', 'hello.txt')


#--upload_file is enough


# import boto3
#
# s3 = boto3.resource('s3')
# bucket = s3.Bucket('000000000000name')
# bucket.wait_until_exists()

#Logging Configs
import logging
import sys
import os
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
logging.getLogger().setLevel(logging.INFO)

import boto3, botocore
s3 = boto3.resource('s3')
bucket_name = 'some-private-bucket'
#bucket_name = 'bucket-to-check'

bucket = s3.Bucket(bucket_name)
def check_bucket(bucket):
    try:
        s3.meta.client.head_bucket(Bucket=bucket)
        print("Bucket Exists!")
        return True
    except botocore.exceptions.ClientError as e:
        # If a client error is thrown, then check that it was a 404 error.
        # If it was a 404 error, then the bucket does not exist.
        error_code = int(e.response['Error']['Code'])
        if error_code == 403:
            print("Private Bucket. Forbidden Access!")
            return True
        elif error_code == 404:
            print("Bucket Does Not Exist!")
            return False

def upload_file(local_file,s3_bucket,s3_key):
    if not check_bucket(s3_bucket):
        logging.error("It seems like S3 bucket check failed.Please make sure bucket exists and proper ACL")



print(os.path.abspath("./s3_upload.py"))
logging.info("hiii")