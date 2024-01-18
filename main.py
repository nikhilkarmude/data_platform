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

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


def split_large_json_file(bucket_name, large_file_path, job_name, num_records, small_file_path):
    job = Job(glueContext)
    job.init(job_name, args)

    # Read the large JSON file from S3 bucket into a DynamicFrame
    json_dyf = glueContext.create_dynamic_frame.from_options(
        's3',
        {'paths': [f's3://{bucket_name}/{large_file_path}']},
        format_options={'withHeader': True, 'inferSchema': True},
        format='json'
    )

    num_partitions = int(json_dyf.count() / num_records)
    if json_dyf.count() % num_records != 0:
        num_partitions += 1

    # Convert DynamicFrame to DataFrame and Repartition
    json_df = json_dyf.toDF()
    json_df = json_df.repartition(num_partitions)

    # Convert DataFrame back to DynamicFrame 
    rebinned_dyf = DynamicFrame.fromDF(json_df, glueContext, "rebinned_dyf")

    # Write the partitions as separate JSON files back to S3
    glueContext.write_dynamic_frame.from_options(
        frame = rebinned_dyf,
        connection_type = "s3",
        connection_options = {"path": f"s3://{bucket_name}/{small_file_path}"},
        format = "json"
    )

    job.commit()

# call the function with required parameters
split_large_json_file(bucket_name='mybucket', large_file_path='large-file.json', job_name='myjob', num_records=5000, small_file_path='small-file.json')

