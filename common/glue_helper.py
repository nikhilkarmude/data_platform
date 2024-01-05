import boto3

class GlueHelper:
    
    def __init__(self):
        self.client = boto3.client('glue')
        
    def create_job(self, job_name, script_location, role, args):
        response = self.client.create_job(
            Name=job_name,
            Role=role,
            Command={'Name': 'glueetl', 'ScriptLocation': script_location},
            DefaultArguments=args
        )
        return response

    def update_job(self, job_name, script_location, role, args):
        response = self.client.update_job(
            JobName=job_name,
            JobUpdate={'Role': role, 'Command': {'Name': 'glueetl', 'ScriptLocation': script_location}, 'DefaultArguments': args}
        )
        return response

    def start_job(self, job_name, args):
        response = self.client.start_job_run(JobName=job_name, Arguments=args)
        return response

    def get_job_status(self, job_run_id):
        response = self.client.get_job_run(JobName=job_name, RunId=job_run_id)
        return response['JobRun']['JobRunState']

# Usage:

helper = GlueHelper()

# Create a glue job
job_response = helper.create_job('my_job', 's3://mybucket/myscript.py', 'myRole', {'--Arg1': 'value1'})

# Update a glue job
update_response = helper.update_job('my_job', 's3://mybucket/myscript_updated.py', 'myRole', {'--Arg1': 'value1'})

# Start a glue job
run_response = helper.start_job('my_job', {'--Arg1': 'value1'})

# Get a glue job status
status = helper.get_job_status(run_response['JobRun']['Id'])
