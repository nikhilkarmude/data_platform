import logging
from framework.ingestion import Ingest
from common.aws_s3 import S3Helper
from common.aws_kms import KMSHelper
from common.aws_secrets_manager import SecretsManagerHelper


logging.basicConfig(filename='mylog.log', level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def source_to_raw():
    """
    This function reads data from a source and places raw data into the system.
    """
    logger.info('Starting the script')
    Ingest.run()    

def raw_to_stage():
    """
    This function reads raw data and prepares it for staging.
    """
    logger.info('Starting the script')
    print("Running raw_to_stage()...")
 
def stage_to_cleansed():
    """
    This function takes staged data and cleanses it.
    """
    print("Running stage_to_cleansed()...")

def cleansed_to_semantic():
    """
    This function takes cleansed data to a semantic model for further analysis.
    """
    print("Running cleansed_to_semantic()...")
    
def main():
    """
    The main function.
    """



if __name__ == "__main__":
    main()



import boto3

s3 = boto3.client('s3')

def get_file_size(bucket, key):
    response = s3.head_object(Bucket=bucket, Key=key)
    size = response['ContentLength']
    return size

bucket_name = 'Your Bucket Name'
file_key = 'File Key or Path'  # This is the path to the file in the S3 bucket

size_in_bytes = get_file_size(bucket_name, file_key)

size_in_megabytes = size_in_bytes / (1024 * 1024)

desired_file_size_mb = 250  # The desired output file size in MB
num_partitions = max(1, round(size_in_megabytes / desired_file_size_mb))  # The number of partitions 

if num_partitions > 1:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName('split_large_file').getOrCreate()

    # read csv file into a dataframe
    df = spark.read.csv('s3://{}/{}'.format(bucket_name, file_key))

    # repartition the dataframe into the desired number of partitions
    df.repartition(num_partitions).write.csv('path_to_output_directory')



