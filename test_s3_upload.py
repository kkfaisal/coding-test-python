import unittest
import s3_upload
import boto3
import os




#Params for testing
# TODO: Pass these as command line args.
LOCAL_FILE = "./test_file.txt"
S3_BUCKET_NAME = 'faisalkk-data-test'
S3_KEY = 'test_upload/ut/small_file_demo'
S3_KEY_2 = 'test_upload/ut/small_file_demo2'
AWS_CONFIG_PROFILE = 'default'

S3_RSRC = None

def delete_s3_object(key):
    session = boto3.Session() #profile_name=AWS_CONFIG_PROFILE
    client = session.client('s3')
    client.delete_object(Bucket=S3_BUCKET_NAME,Key=key)


def setUpModule():
    global S3_RSRC

    s3_session = boto3.Session(profile_name=AWS_CONFIG_PROFILE)
    S3_RSRC = s3_session.resource('s3')

    #Create a local file
    with open(LOCAL_FILE,'w') as fl:
        fl.write("This file will be uploaded to s3 for testing...")


def tearDownModule():
    # Remove local file
    os.remove(LOCAL_FILE)

class TestS3Upload(unittest.TestCase):

    def test_upload_new_key(self):
        # Delete object if any and make sure it deleted.
        delete_s3_object(S3_KEY)
        self.assertEqual(s3_upload.s3_key_exists(S3_RSRC, S3_BUCKET_NAME, S3_KEY), False)

        # Upload new file
        s3_upload.upload_file_to_s3(LOCAL_FILE, S3_BUCKET_NAME, S3_KEY, allow_overwrite=True,
                                    profile=AWS_CONFIG_PROFILE)
        # Now key should be present
        self.assertEqual(s3_upload.s3_key_exists(S3_RSRC, S3_BUCKET_NAME, S3_KEY), True)


    def test_upload_existing_key(self):

        # Upload a file with a key
        s3_upload.upload_file_to_s3(LOCAL_FILE,S3_BUCKET_NAME,S3_KEY_2,allow_overwrite=True,profile=AWS_CONFIG_PROFILE)
        self.assertEqual(s3_upload.s3_key_exists(S3_RSRC, S3_BUCKET_NAME, S3_KEY_2), True)

        # Again try to upload with same key with allow_overwrite=False.It should fail.
        with self.assertRaises(s3_upload.S3KeyExistsError):
            s3_upload.upload_file_to_s3(LOCAL_FILE, S3_BUCKET_NAME, S3_KEY_2, allow_overwrite=False,profile=AWS_CONFIG_PROFILE)
        delete_s3_object(S3_KEY_2)

if __name__ == '__main__':
    unittest.main()