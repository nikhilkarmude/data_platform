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
from pyspark.context import SparkContext
from awsglue.context import GlueContext

bucket_name = 'input-bucket-name' # replace with your bucket name
file_key = 'path-to-input-file' # replace with your file path
output_folder = 'output-folder-name' # replace with your output folder name

def get_file_size(bucket, key):
    s3 = boto3.client('s3')
    response = s3.head_object(Bucket=bucket, Key=key)
    size = response['ContentLength']
    return size

size_in_bytes = get_file_size(bucket_name, file_key)
size_in_megabytes = size_in_bytes / (1024 * 1024)

desired_file_size_mb = 250 # replace with your desired chunk size in MB
num_partitions = max(1, round(size_in_megabytes / desired_file_size_mb))

sc = SparkContext()
glueContext = GlueContext(sc)

# Reading the file from S3 using GlueContext
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [f"s3://{bucket_name}/{file_key}"]},
    format="csv")

# Repartitioning DynamicFrame
dynamic_frame = DynamicFrame.fromDF(dynamic_frame.toDF().repartition(num_partitions), glueContext, 'dynamic_frame')

# Writing dynamic frame to S3 in CSV format
glueContext.write_dynamic_frame.from_options(frame = dynamic_frame,
                                             connection_options = {"path": f's3://{bucket_name}/{output_folder}'},
                                             format = "csv", transformation_ctx = "datasink")




