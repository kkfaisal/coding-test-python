"""
Solution to Question1.
By defult file uploaded will overwrite if key is already existing.
There is a flag allow_overwrite ,if set to False,will upload the file only if key is not present in s3.
Unit test cases are included.
"""

import logging
import os
import boto3, botocore

FORMAT = "[%(levelname)s:%(filename)s:%(lineno)s %(funcName)s()] --> %(message)s"
logging.basicConfig(format=FORMAT)
logging.getLogger().setLevel(logging.INFO)


class S3KeyExistsError(Exception):
    def __init__(self, message):
        super().__init__(message)


def bucket_exists(s3_rsrc,bucket_name):
    '''
    Check S3 bucket exists and have proper permission.
    :param bucket:
    :return Bool : True if Bucket exists with proper permissions.
    '''
    try:
        s3_rsrc.meta.client.head_bucket(Bucket=bucket_name)
        return True
    except botocore.exceptions.ClientError as e:
        logging.info("Bucket does not exists or No permission to access it. Error : {error}".format(error=e.response['Error']))
    return False


def s3_key_exists(s3_rsrc,bucket_name,key):
    """
    To check if a key exists in bucket
    :param bucket_name:
    :param key:
    :return:Bool.
    """
    try:
        resp=s3_rsrc.meta.client.head_object(Bucket=bucket_name,Key=key)
        if len(resp)>0:
            logging.info("The given key {key} already exists.Last Modified on {date}".format(key=key,date=resp['LastModified']))
            return True
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False


def upload_file_to_s3(local_file,s3_bucket_name,s3_key,allow_overwrite=True,profile='default'):
    '''
    Upload a local file to S3 buacket.
    :param local_file:file path
    :param s3_bucket_name: Bucket to which file to be uploaded
    :param s3_key:Key of s3 object
    :param profile:Profile in AWS config/credential file.
    :param allow_overwrite : Allow overwrite existing key
    :return: None ,Raise exception if uploading is failed.
    '''
    s3_session= boto3.Session(profile_name=profile)
    s3_rsrc = s3_session.resource('s3')

    if not bucket_exists(s3_rsrc,s3_bucket_name):
        logging.error("It seems like S3 bucket check failed.Please make sure bucket exists and proper AC")
        return

    abs_path =os.path.abspath(local_file)
    logging.info("Trying to access local file : {file}".format(file=abs_path))
    if not os.path.isfile(abs_path):
        logging.error("Given path do not pointing to a file.Please make sure path exists and pointing to a file")
        raise IOError("Failed to read file")

    if not allow_overwrite:
        if s3_key_exists(s3_rsrc,s3_bucket_name,s3_key):
            logging.error("Key {key}  alreadey exists and allow_overwrite is false.Please try other key or set allow_overwirite to true".format(key=s3_key))
            raise S3KeyExistsError("Key already exists in S3 bucket")

    try:
        s3_rsrc.meta.client.upload_file(abs_path, s3_bucket_name,s3_key)
    except Exception :
        logging.error("Error when trying to upload File to S3 :")
        raise Exception
    else:
        logging.info("File upload completed successfully -> Bucket:{bckt},Key:{key}".format(bckt=s3_bucket_name,key=s3_key))


if __name__ == '__main__':
    s3_bucket_name="faisalkk-data-test"
    local_file_path="./README.md"
    s3_key='testing/demo/file'
    upload_file_to_s3(local_file_path,s3_bucket_name,s3_key,allow_overwrite=True)
