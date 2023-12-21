import json
import logging
from aws_helper import AWSHelper

import json
import logging
from aws_helper import AWSHelper

class SecretsManagerHelper:
    def __init__(self, profile=None):
        """
        Initialize AWS Secrets Manager client.
        
        Parameters:
        profile (str): The AWS CLI profile name. Default value is None.
                       'None' will use the default AWS CLI profile.

        Usage:
        secretsmanager_helper = SecretsManagerHelper(profile='my-profile')
        """
        logging.info('Initializing Secrets Manager client...')
        aws_helper = AWSHelper()
        secrets_manager_session = aws_helper.create_boto3_session(aws_profile=profile)
        self.sm_client = secrets_manager_session.client('secretsmanager')
        logging.info('Secrets Manager client initialized.')
        
    def get_secret(self, secret_name):
        """
        Get the secret value. This example assumes the secret value is stored as a JSON string.

        Parameters:
        secret_name (str): The name of the secret.

        Returns:
        dict: The secret value.

        Usage:
        secret = secretsmanager_helper.get_secret('my-secret')
        """
        logging.info(f'Retrieving secret {secret_name}...')
        response = self.sm_client.get_secret_value(SecretId=secret_name)
        secret_value = json.loads(response['SecretString'])
        logging.info(f'Secret {secret_name} successfully retrieved.')
        return secret_value

    def create_secret(self, secret_name, secret_value):
        """
        Create a new secret. This example assumes the secret value is a JSON string.
        
        Parameters:
        secret_name (str): The name of the secret.
        secret_value (dict): A dictionary representing the secret value.

        Usage:
        secretsmanager_helper.create_secret('my-secret', {"username":"my-username", "password":"my-password"})
        """
        logging.info(f'Creating secret {secret_name}...')
        self.sm_client.create_secret(
            Name=secret_name,
            SecretString=json.dumps(secret_value)
        )
        logging.info(f'Secret {secret_name} successfully created.')

    def update_secret(self, secret_name, secret_value):
        """
        Update the value of an existing secret. This example assumes the secret value is a JSON string.
        
        Parameters:
        secret_name (str): The name of the secret.
        secret_value (dict): A dictionary representing the new secret value.

        Usage:
        secretsmanager_helper.update_secret('my-secret', {"username":"new-username", "password":"new-password"})
        """
        logging.info(f'Updating secret {secret_name}...')
        self.sm_client.update_secret(
            SecretId=secret_name,
            SecretString=json.dumps(secret_value)
        )
        logging.info(f'Secret {secret_name} successfully updated.')

    def delete_secret(self, secret_name):
        """
        Delete a secret.

        Parameters:
        secret_name (str): The name of the secret.

        Usage:
        secretsmanager_helper.delete_secret('my-secret')
        """
        logging.info(f'Deleting secret {secret_name}...')
        self.sm_client.delete_secret(
            SecretId=secret_name
        )
        logging.info(f'Secret {secret_name} successfully deleted.')

# # Usage
# secrets_helper = SecretsManagerHelper(profile="default")

# # Create a secret
# secret_name = 'my_secret1'
# secret_value = {'username': 'my_username', 'password': 'my_password'}
# secrets_helper.create_secret(secret_name, secret_value)

# # Get a secret
# print(secrets_helper.get_secret(secret_name))

# # Update a secret
# secret_value = {'username': 'new_username', 'password': 'new_password'}
# secrets_helper.update_secret(secret_name, secret_value)

# # Delete a secret
# secrets_helper.delete_secret(secret_name)
