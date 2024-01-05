import boto3

def upload_to_s3(bucket_name, script_name):
    """
    Uploads a local file to an S3 bucket

    Parameters:
    bucket_name (str): Name of the S3 bucket
    script_name (str): Name of the local script file
    """
    s3 = boto3.client('s3')
    s3.upload_file(script_name, bucket_name, script_name)

    print(f"Uploaded {script_name} to {bucket_name}")

def trigger_glue_job(job_name, script_arguments):
    """
    Triggers an AWS Glue job

    Parameters:
    job_name (str): Name of the Glue job to be triggered
    script_arguments (dict): Arguments to be passed to the Glue job
    """
    glue = boto3.client('glue')
    response = glue.start_job_run(JobName=job_name, Arguments=script_arguments)

    print(f"Triggered {job_name}, Job Run ID: {response['JobRunId']}")

if __name__ == "__main__":
    bucket_name = 'your_bucket'
    script_name = 'myscript.py'
    job_name = 'your_glue_job'

    upload_to_s3(bucket_name, script_name)

    # Define the additional Python modules that you want to use inside the Glue job
    python_modules = 'boto3,sqlalchemy,psycopg2'

    # Define parameters for the Glue job, including the script location and the additional Python modules
    script_arguments = {
        '--additional-python-modules': python_modules,
        '--JOB_NAME': job_name,
    }

    trigger_glue_job(job_name, script_arguments)



import boto3

def upload_to_s3(bucket_name, file_name, object_name=None):
    """
    Upload a file to S3 bucket

    bucket_name: Name of the S3 bucket
    file_name: File to upload
    object_name: S3 object name. If not specified then `file_name` is used
    """
    s3 = boto3.client('s3')
    s3.upload_file(file_name, bucket_name, object_name or file_name)

    print(f"Uploaded {file_name} to s3://{bucket_name}/{object_name or file_name}")

def trigger_glue_job(job_name, script_arguments):
    """
    Triggers an AWS Glue job

    job_name (str): Name of the Glue job to be triggered
    script_arguments (dict): Arguments to be passed to the Glue job
    """
    glue = boto3.client('glue')
    response = glue.start_job_run(JobName=job_name, Arguments=script_arguments)

    print(f"Triggered {job_name}, Job Run ID: {response['JobRunId']}")

if __name__ == "__main__":
    bucket_name = 'your_bucket'
    script_name = 'myscript.py'
    job_name = 'your_glue_job'

    # Upload the script
    upload_to_s3(bucket_name, script_name)

    # Upload the additional Python packages
    python_packages = ['package1.whl', 'package2.whl', 'package3.zip']
    for package in python_packages:
        upload_to_s3(bucket_name, package)

    # Define parameters for the Glue job, including the script location and the Python packages location
    script_arguments = {
        '--additional-python-modules': ','.join(f"s3://{bucket_name}/{package}" for package in python_packages), 
        '--JOB_NAME': job_name,
    }

    trigger_glue_job(job_name, script_arguments)
