from common.aws_helper import AWSHelper
import logging

logging.basicConfig(level=logging.INFO)

class S3Helper:
    def __init__(self, profile=None):
        """Initialize S3 client
        Parameters: 
        profile (str): The AWS CLI profile name. Default value is None.
                        'None' will use the default AWS CLI profile.

        Usage:
        s3_helper = S3Helper(profile="default")
        """
        logging.info('Initializing S3 client...')
        aws_helper = AWSHelper()
        s3_session = aws_helper.create_boto3_session(aws_profile=profile)
        self.s3 = s3_session.client('s3')
        logging.info('S3 client initialized.')

    def list_buckets(self):
        """List all bucket names
        Returns: 
        list: A list of bucket names.

        Usage:
        print(s3_helper.list_buckets())
        """
        logging.info('Listing all bucket names...')
        response = self.s3.list_buckets()
        bucket_names = [bucket['Name'] for bucket in response['Buckets']]
        logging.info(f"Found {len(bucket_names)} buckets.")
        return bucket_names

    def bucket_exists(self, bucket_name):
        """Check if a bucket exists
        Parameters: 
        bucket_name (str): The name of the bucket to check.

        Returns:
        bool: True if bucket exists, False otherwise.
        """
        logging.info(f'Checking if bucket {bucket_name} exists...')
        try:
            self.s3.head_bucket(Bucket=bucket_name)
            return True
        except Exception as e:
            logging.error(f'Bucket {bucket_name} does not exist. Error: {str(e)}')
            return False
    
    def create_bucket(self, bucket_name):
        """Create a new bucket
        Parameters: 
        bucket_name (str): The name of the new bucket.

        Usage:
        s3_helper.create_bucket('new-bucket-name')
        """
        if not self.bucket_exists(bucket_name):
            logging.info(f'Creating bucket {bucket_name}...')
            self.s3.create_bucket(Bucket=bucket_name)
            logging.info(f'Bucket {bucket_name} created.')
        else:
            logging.info(f'Bucket {bucket_name} already exists.')

    def delete_bucket(self, bucket_name):
        """Delete a bucket
        Parameters: 
        bucket_name (str): The name of the bucket to delete.

        Usage:
        s3_helper.delete_bucket('bucket-name')
        """
        logging.info(f'Deleting bucket {bucket_name}...')
        self.s3.delete_bucket(Bucket=bucket_name)
        logging.info(f'Bucket {bucket_name} deleted.')

    def list_objects(self, bucket_name):
        """List all objects in a bucket
        Parameters: 
        bucket_name (str): The name of the bucket to list objects from.

        Returns:
        list: A list of object keys.

        Usage:
        print(s3_helper.list_objects('bucket-name'))
        """
        logging.info(f'Listing all objects in bucket {bucket_name}...')
        response = self.s3.list_objects(Bucket=bucket_name)
        object_keys = [item['Key'] for item in response.get('Contents', [])]
        logging.info(f"Found {len(object_keys)} objects in bucket {bucket_name}.")
        return object_keys
    
    def object_exists(self, bucket_name, object_name):
        """Check if an object exists in a bucket
        Parameters: 
        bucket_name (str): The name of the bucket to check.
        object_name (str): The key of the object to check.

        Returns:
        bool: True if object exists, False otherwise.
        """
        logging.info(f'Checking if object {object_name} exists in bucket {bucket_name}...')
        try:
            self.s3.head_object(Bucket=bucket_name, Key=object_name)
            return True
        except Exception as e:
            logging.error(f'Object {object_name} does not exist in bucket {bucket_name}. Error: {str(e)}')
            return False

    def upload_file(self, bucket_name, object_name, overwrite=True, file_obj=None, file_path=None):
        """
        Upload a file to a bucket using either a file object or a file path.

        Parameters: 
        bucket_name (str): The name of the bucket to upload to.
        object_name (str): The key for the object to create.
        overwrite (bool): Whether to overwrite the file if it already exists. Defaults to True.
        file_obj (file-like object): File-like object to upload, could be an open file or a BytesIO object. Defaults to None.
        file_path (str): Path to the file to upload. Used if file_obj is not provided. Defaults to None.

        Usage 1: File is a file-like object 
        with open('/local/path/to/myfile.txt', 'rb') as file:
            s3_helper.upload_file('bucket-name', 'object-name', file_obj=file)

        Usage 2: File is on disk and you have the path to it
        s3_helper.upload_file('bucket-name', 'object-name', file_path='/local/path/to/myfile.txt')

        Usage 3: Uploading file into a specific directory structure in the bucket
        Let's say you have a file in your local system at path '/path/to/local/file.txt'
        And you want to upload this file into 'bucket-name' bucket in 'folder/subfolder' directory with name 'my-object'

        # Now upload the file
        s3_helper.upload_file('bucket-name', 'folder/subfolder/my-object', file_path='/path/to/local/file.txt')

        # If the execution is successful, your file should now be uploaded to S3.
        # You should be able to access it at s3://bucket-name/folder/subfolder/my-object
        """
        logging.info(f'Uploading file to bucket {bucket_name} as object {object_name}...')
        if not self.bucket_exists(bucket_name):
            logging.info(f'Bucket {bucket_name} does not exist. Creating it...')
            self.create_bucket(bucket_name)
        if not overwrite and self.object_exists(bucket_name, object_name):
            raise Exception(f'Object {object_name} already exists in bucket {bucket_name} and overwrite is set to False.')

        # If file_obj was provided, use it, else open the file at file_path
        if file_obj is None:
            if file_path is None:
                raise ValueError("Must provide one of 'file_obj' or 'file_path'")
            with open(file_path, 'rb') as f:
                self.s3.upload_fileobj(f, bucket_name, object_name)
        else:
            self.s3.upload_fileobj(file_obj, bucket_name, object_name)

        logging.info(f'Successfully uploaded file to {bucket_name} as {object_name}.')

    def download_file(self, bucket_name, object_name, file_path):
        """Download an object from a bucket and save it as a file.
        
        NOTE: This function will overwrite an existing file without warning if file_path points to an existing file.

        Parameters:
        bucket_name (str): The name of the bucket to download from.
        object_name (str): The key of the object to download.
        file_path (str): The local path where the downloaded file should be saved.

        Usage:
        s3_helper.download_file('bucket-name', 'object-name', '/local/path/to/save/file.txt')
        """
        logging.info(f'Downloading object {object_name} from bucket {bucket_name} to {file_path}...')
        with open(file_path, 'wb') as file:
            self.s3.download_fileobj(bucket_name, object_name, file)
        logging.info(f'Successfully downloaded file to {file_path}.')

    def delete_object(self, bucket_name, object_name):
        """Delete an object from a bucket
        Parameters: 
        bucket_name (str): The name of the bucket which has the object to delete.
        object_name (str): The key of the object to delete.

        Usage:
        s3_helper.delete_object('bucket-name', 'object-name')
        """
        logging.info(f'Deleting object {object_name} from bucket {bucket_name}...')
        self.s3.delete_object(Bucket=bucket_name, Key=object_name)
        logging.info(f'Object {object_name} deleted from bucket {bucket_name}.')

    # Add rename_file_name method to the class
    def rename_file_name(self, bucket_name, existing_file_name, new_file_name):
        """Rename an existing file in a given S3 bucket.

        This method copies the existing file to new file and then deletes
        the existing file.

        Args:
        bucket_name (str): The name of the bucket with the file.
        existing_file_name (str): The existing name of the file.
        new_file_name (str): The new name of the file.
        """
        logging.info(f'Renaming file {existing_file_name} to {new_file_name}...')
        # Copy the file to new file
        self.s3.copy_object(
            Bucket=bucket_name,
            CopySource={'Bucket': bucket_name, 'Key': existing_file_name},
            Key=new_file_name)
        # Removing the old file
        self.s3.delete_object(Bucket=bucket_name, Key=existing_file_name)

    # Add move_files method to the class
    def move_files(self, bucket_name, new_bucket_name, file_name):
        """Move a given file from one S3 bucket to another.

        This method copies the existing file from the existing_bucket to 
        new_bucket and then deletes the existing file from existing_bucket.

        Args:
        bucket_name (str): The name of the existing bucket with the file.
        new_bucket_name (str): The name of the new bucket to which the file is to be moved.
        file_name (str): The name of the file.
        """
        logging.info(f'Moving file {file_name} from bucket {bucket_name} to {new_bucket_name}')
        
        # Copy the file to new bucket
        self.s3.copy_object(
            Bucket=new_bucket_name,
            CopySource={'Bucket': bucket_name, 'Key': file_name},
            Key=file_name)
        
        # Removing the file from old bucket
        self.s3.delete_object(Bucket=bucket_name, Key=file_name)

# # Instantiate the class for a specific profile
# s3_helper = S3Helper(profile='default')

# # Create a bucket
# bucket_name = 'nikhilkarmude'
# s3_helper.create_bucket(bucket_name)

# # Verify the bucket is created by checking its existence
# assert s3_helper.bucket_exists(bucket_name) == True

# # List all buckets
# all_buckets = s3_helper.list_buckets()
# print(f"All buckets: {all_buckets}")

# # Upload a file to the bucket from a file path
# s3_helper.upload_file(bucket_name, 'my-object', file_path='/Users/nikhilkarmude/Library/CloudStorage/OneDrive-EY/Documents/workspace/BR_CDP/setup/requirements.txt')

# # Verify the object is uploaded by checking its existence
# assert s3_helper.object_exists(bucket_name, 'my-object') == True

# # List all objects in the bucket
# all_objects = s3_helper.list_objects(bucket_name)
# print(f"All objects in {bucket_name}: {all_objects}")

# # Download the file
# s3_helper.download_file(bucket_name, 'my-object', '/Users/nikhilkarmude/Library/CloudStorage/OneDrive-EY/Documents/workspace/BR_CDP/setup/requirements.txt')

# # Delete the object from the bucket
# s3_helper.delete_object(bucket_name, 'my-object')

# # Verify the object is deleted
# assert s3_helper.object_exists(bucket_name, 'my-object') == False

# # Delete the bucket
# s3_helper.delete_bucket(bucket_name)

# # Verify the bucket is deleted
# assert s3_helper.bucket_exists(bucket_name) == False

# print("All operations completed successfully.")
