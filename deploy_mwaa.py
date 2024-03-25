import boto3
import zipfile
import os
import shutil
import subprocess

s3 = boto3.client('s3')

bucket_name = 'my-bucket'  # replace with your bucket name
object_key = 'my-object.zip'  # replace with your s3 object key

local_zip = '/path/to/where/to/download/object.zip'  # replace with your local file path
local_dir_to_extract_to = '/path/to/where/to/extract/files'  # replace with your local directory to extract zip to
wheel_dir = '/path/to/where/to/store/wheel'  # replace with your local directory to store wheel file

wheel_file = 'finance_data_platform.whl'
deployment_file = 'deploy.py'

# Download the zip file from S3
with open(local_zip, 'wb') as data:
    s3.download_fileobj(bucket_name, object_key, data)

with zipfile.ZipFile(local_zip, 'r') as zip_ref:
    zip_ref.extractall(local_dir_to_extract_to)

os.chdir(local_dir_to_extract_to)

# Path to the old wheel file
old_wheel_path = os.path.join(local_dir_to_extract_to, wheel_file)

# Delete the old wheel file
if os.path.exists(old_wheel_path):
    os.remove(old_wheel_path)

# Build the new wheel file
subprocess.check_call(["python", deployment_file, "bdist_wheel", "-d", local_dir_to_extract_to])
new_wheel_path = os.path.join(local_dir_to_extract_to, wheel_file)

# Check if wheel file is created successfully
if os.path.exists(new_wheel_path):

    # Now zip everything back
    shutil.make_archive(local_dir_to_extract_to, 'zip', local_dir_to_extract_to)

    # Then reupload the zip back to the s3
    with open(local_zip, 'rb') as data:
        s3.upload_fileobj(data, bucket_name, object_key)

else:
    print("New wheel file was not created successfully. Please check!")
