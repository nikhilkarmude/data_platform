import boto3
import os
import logging

class AWSHelper:
    """A helper class for AWS."""

    def __init__(self):
        logging.info('Creating AWSHelper...')
        
    def create_boto3_session(self, aws_profile=None, role_arn=None, region=None):
        """
        Creates a Boto3 session.

        Parameters:
        aws_profile (str): The name of an AWS CLI profile that contains credentials. Default is None.
        role_arn (str): The Amazon Resource Name (ARN) of an IAM role to assume. Default is None.
        region (str): The name of the AWS Region to use. Default is None.

        Returns:
        boto3.Session: A Boto3 session.

        Usage:
        aws_helper = AWSHelper()
        session = aws_helper.create_boto3_session(aws_profile='my-profile') 
        - or - 
        session = aws_helper.create_boto3_session(role_arn='arn:aws:iam::012345678901:role/my-role', region='us-west-2')
        
        sts = session.client('sts')
        print(sts.get_caller_identity())
        """
        logging.info('Creating Boto3 session...')
        if 'AWS_EXECUTION_ENV' in os.environ:
            # AWS Glue environment (IAM role assumed automatically)
            logging.info('AWS Glue environment detected. Creating a Boto3 session...')
            session = boto3.Session(region_name=region)
        else:
            # Local development environment or other scenarios
            if aws_profile:
                # Use the specified AWS CLI profile to get credentials
                logging.info(f'Creating a Boto3 session with profile {aws_profile}...')
                session = boto3.Session(region_name=region, profile_name=aws_profile)
            elif role_arn:
                # Assume the specified IAM role
                logging.info(f'Creating a Boto3 session by assuming role {role_arn}...')
                sts_client = boto3.client('sts')
                assumed_role = sts_client.assume_role(RoleArn=role_arn, RoleSessionName='AssumedRoleSession')
                session_params = {
                    'aws_access_key_id': assumed_role['Credentials']['AccessKeyId'],
                    'aws_secret_access_key': assumed_role['Credentials']['SecretAccessKey'],
                    'aws_session_token': assumed_role['Credentials']['SessionToken'],
                    'region_name': region
                }
                session = boto3.Session(**session_params)
            else:
                raise ValueError("Either 'aws_profile' or 'role_arn' must be provided.")
        logging.info('Successfully created Boto3 session.')
        return session
