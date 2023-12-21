import sys, os

# below lines are added so that we can run this file as - python ingestion.py
# without facing module not found error.
# If these lines are removed, then below import statements need to be modified.
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)


import uuid
from abc import ABC
from common.aws_s3 import S3Helper
from common.aws_kms import KMSHelper
from common.aws_secrets_manager import SecretsManagerHelper

class Ingest:
    def __init__():
        pass

    def get_process_run_id():
        return uuid.uuid4()
    
    def run():
        s3_helper = S3Helper(profile='default') 
        #kms_helper = KMSHelper(profile='default')
        #secrets_helper = SecretsManagerHelper(profile="default")
        #abc = ABC()

        

    if __name__ == "__main__":
        run()

'''
Wrapper 1: source to raw s3
1. Get object_identifier and system_name pairs from process_object_lookup table.
    Logic: object_name should not contain "_"

2. Get parameters from parameter config table for system-object pairs.

3. Generate a unique process identifier using uuid

4. For each system-object pair: 
    Insert record into process run table using details in 1 and 3
    based on DB/API/SFTP approach get the connect details and pass to the glue
    ## GLUE Wrapper starts
    invoke the glue script containing load_source_to_s3 in csv format
    note: Glue will log into cloud watch
    ## GLUE Wrapper ends

    # using cloud watch api pull/read the log for any exception details and update run table record

5. if successfull extraction from source to raw s3 then insert record in data feed load table with s3 file details
    
Wrapper 2: raw s3 to stage
1. Inputs:  unique process identifier, system-object pairs, file paths
2. run copy command to move file from given file paths to stage s3.
    

'''

