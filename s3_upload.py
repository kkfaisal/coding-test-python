
import logging
import os
import boto3, botocore

FORMAT = "[%(levelname)s:%(filename)s:%(lineno)s-%(funcName)s()] --> %(message)s"
logging.basicConfig(format=FORMAT)
logging.getLogger().setLevel(logging.INFO)

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
    '''
    To check if a key exists in bucket
    :param s3_bucket:
    :param key:
    :return:Bool.
    '''
    # try:
    #     s3_bucket.Object(key).get()
    # except botocore.exceptions.ClientError as ex:
    #     if ex.response['Error']['Code'] == 'NoSuchKey':
    #         return False
    # return True

    try:
        resp=s3_rsrc.meta.client.head_object(Bucket=bucket_name,Key=key)
        if len(resp)>0:
            return True
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] != '404':
            return False


def upload_file_to_s3(local_file,s3_bucket_name,s3_key,allow_overwirite=True,profile='default'):
    '''
    Upload a loacl file to S3 buacket.
    :param local_file:file path
    :param s3_bucket: Bucket to which file to be uploaded
    :param s3_key:Key of s3 object
    :param profile:Profile in AWS config/credential file.
    :param allow_overwirite : Allow overwrite existing key
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
        return

    if not allow_overwirite:
        if s3_key_exists(s3_rsrc,s3_bucket_name,s3_key):
            logging.error("Key {key}  alreadey exists and allow_overwirite is false.Please try other key or set allow_overwirite to true".format(key=s3_key))
            return

    try:
        s3_rsrc.meta.client.upload_file(abs_path, s3_bucket_name,s3_key)
    except Exception :
        logging.error("Error when trying to upload File to S3")
        logging.exception(Exception)
    else:
        logging.info("File upload completed successfully -> Bucket:{bckt},Key:{key}".format(bckt=s3_bucket_name,key=s3_key))




if __name__=='__main__':
    upload_file_to_s3("./.gitignore",'lens-dw-stag-m4m','lklklkl',allow_overwirite=False)
    # bucket_exists("hhhhhhhhhhh")
