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



from pyspark.context import SparkContext
from awsglue.context import GlueContext

sc = SparkContext()
glueContext = GlueContext(sc)

# Define your input folder
input_folder = 's3://path_to_input_folder/*'

# Read all the files in the input folder into a DynamicFrame
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3", 
    connection_options={"paths": [input_folder]}, 
    format="csv"
)

# Coalesce the DynamicFrame to one partition
coalesced_dyf = dynamic_frame.coalesce(1)

# Define your output folder (don't include filename)
output_folder = 's3://path_to_output_folder'

# Export data as a single CSV file
glueContext.write_dynamic_frame.from_options(
    frame = coalesced_dyf, 
    connection_type = "s3", 
    connection_options = {"path": output_folder},
    format = "csv"
)




