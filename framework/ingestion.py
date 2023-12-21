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

