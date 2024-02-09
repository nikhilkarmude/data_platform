import boto3

class MWAAHelper:

    def __init__(self, region, profile=None):
        """ Initialize MWAA client.
        Parameters:
        region (str): The AWS region.
        profile (str): The AWS CLI profile name. Default is None.
                       'None' will use the default AWS CLI profile.
        Usage:
        mwaa_helper = MWAAHelper(region='us-east-1', profile='default')
        """
        session = boto3.Session(region_name=region, profile_name=profile)
        self.mwaa_client = session.client('mwaa')

    def list_environments(self):
        """ List all MWAA environments
        Returns:
        list: A list of MWAA environment names.
        Usage:
        all_environments = mwaa_helper.list_environments()
        """
        response = self.mwaa_client.list_environments(MaxResults=100)
        return response['Environments']

    def get_environment(self, name):
        """ Get MWAA environment details.
        Parameters:
        name (str): The name of the MWAA environment
        Returns:
        dict: The details of the MWAA environment.
        Usage:
        environment_detail = mwaa_helper.get_environment('MyEnvironment')
        """
        response = self.mwaa_client.get_environment(Name=name)
        return response['Environment']

