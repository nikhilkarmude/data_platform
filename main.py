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

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init('json-job', args)

# Specify your bucket name and prefix
bucket_name = 'your-bucket-name'
prefix = 'input/'

# Create a DynamicFrame from all the JSON files under the prefix in the S3 bucket
json_dyf = glueContext.create_dynamic_frame.from_catalog(database='json_db',
                                                          table_name='json_table',
                                                          transformation_ctx='json_dyf')

# Identify JSON schema and flatten it 
relationalized_json_dyf = json_dyf.relationalize("root", "s3://"+bucket_name+"/tempDir/")

# Loop over the DynamicFrames in the relationalized DynamicFrameCollection
for df_name, json_df in relationalized_json_dyf.items():
    # Convert from DynamicFrame to DataFrame
    json_data_df = json_df.toDF()
    
    # Repartition DataFrame
    num_records = 5000
    num_partitions = int(json_data_df.count() / num_records)
    if json_data_df.count() % num_records != 0: num_partitions += 1
    json_data_df = json_data_df.repartition(num_partitions)
    
    # Convert DataFrame back to DynamicFrame 
    json_combined_dyf = DynamicFrame.fromDF(json_data_df, glueContext, "json_combined_dyf")

    # Save as separate JSON files
    glueContext.write_dynamic_frame.from_options(
        frame = json_combined_dyf,
        connection_type = "s3",
        connection_options = {"path": f"s3://{bucket_name}/output/{df_name}" },
        format = "json"
    )

job.commit()
