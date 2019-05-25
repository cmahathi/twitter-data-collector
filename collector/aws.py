from collector import utils
import datetime
import os
import shutil
from boto.s3.connection import S3Connection
from boto.s3.key import Key

AWS = 'aws'
AWS_ACCESS_KEY = 'aws_access_key'
AWS_SECRET_KEY = 'aws_secret_key'
AWS_BUCKET = 'aws_bucket'
AWS_FOLDER = 'aws_folder'
AWS_HOST = 's3.us-west-2.amazonaws.com'


def move_to_s3(db_name: str, archive_name: str):
    today = datetime.datetime.today()
    path = os.getcwd()
    keys = utils.load_keys()[AWS]

    aws_access_key = keys[AWS_ACCESS_KEY]
    aws_secret_key = keys[AWS_SECRET_KEY]
    aws_bucket = keys[AWS_BUCKET]
    aws_folder = keys[AWS_FOLDER]

    # configuring filepath and tar file name
    archive_path = os.path.join(path, archive_name)
    print(f'[FILE] Creating archive for {db_name}')

    shutil.make_archive(archive_path, 'gztar', path)
    print('Completed archiving database')

    full_archive_path = archive_path + '.tar.gz'
    full_archive_name = archive_name + '.tar.gz'

    # Establish S3 Connection
    s3 = S3Connection(aws_access_key, aws_secret_key, AWS_HOST)
    bucket = s3.get_bucket(aws_bucket, validate=False)

    # Send files to S3
    print(f'[S3] Uploading file archive {full_archive_name}')
    k = Key(bucket)
    k.key = today + '/' + full_archive_name
    print(k.key)
    k.set_contents_from_filename(full_archive_path)
    k.set_acl("public-read")
    print(f'[S3] Success uploaded file archive {full_archive_name}')

