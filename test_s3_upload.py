import unittest
import s3_upload
import sys
import boto3
import os
import time

LOCAL_FILE = ''
S3_BUCKET_NAME = ''
S3_KEY = ''
S3_KEY_2 = ''
AWS_CONFIG_PROFILE = ''
S3_RSRC = None

def delete_s3_object(key):
    session = boto3.Session() #profile_name=AWS_CONFIG_PROFILE
    client = session.client('s3')
    client.delete_object(Bucket=S3_BUCKET_NAME,Key=key)


def setUpModule():
    global LOCAL_FILE
    global S3_BUCKET_NAME
    global S3_KEY
    global AWS_CONFIG_PROFILE
    global S3_RSRC
    global S3_KEY_2

    LOCAL_FILE = "./test_file.txt"
    S3_BUCKET_NAME = 'lens-dw-stag-m4m'
    S3_KEY = 'test_upload/ut/small_file_demo'
    S3_KEY_2 = 'test_upload/ut/small_file_demo2'
    AWS_CONFIG_PROFILE = 'default'

    s3_session = boto3.Session(profile_name=AWS_CONFIG_PROFILE)
    S3_RSRC = s3_session.resource('s3')

    # Delete object if present.
    # delete_s3_object()

    #Create a local file
    with open(LOCAL_FILE,'w') as fl:
        fl.write("This file will be uploaded to s3 for testing...")


def tearDownModule():
    # Remove local file
    os.remove(LOCAL_FILE)

class TestS3Upload(unittest.TestCase):

    def upload_new_key(self):
        delete_s3_object(S3_KEY)
        self.assertEqual(s3_upload.s3_key_exists(S3_RSRC, S3_BUCKET_NAME, S3_KEY), False)
        s3_upload.upload_file_to_s3(LOCAL_FILE, S3_BUCKET_NAME, S3_KEY, allow_overwrite=True,
                                    profile=AWS_CONFIG_PROFILE)
        self.assertEqual(s3_upload.s3_key_exists(S3_RSRC, S3_BUCKET_NAME, S3_KEY), False)


    def test_upload_existing_key(self):
        #Upload a file with key
        s3_upload.upload_file_to_s3(LOCAL_FILE,S3_BUCKET_NAME,S3_KEY_2,allow_overwrite=True,profile=AWS_CONFIG_PROFILE)
        self.assertEqual(s3_upload.s3_key_exists(S3_RSRC, S3_BUCKET_NAME, S3_KEY_2), True)

        #Again try to upload with same key
        with self.assertRaises(s3_upload.S3KeyExistsError):
            s3_upload.upload_file_to_s3(LOCAL_FILE, S3_BUCKET_NAME, S3_KEY_2, allow_overwrite=False,profile=AWS_CONFIG_PROFILE)
        delete_s3_object(S3_KEY_2)

if __name__ == '__main__':
    unittest.main()