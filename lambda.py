import json
import boto3

def lambda_handler(event, context):
    # 200 is the HTTP status code for "ok".
    status_code = 200

    try:
        # From the input parameter named "event", get the body, which contains
        # the input rows.
        event_body = event["body"]

        # Convert the input from a JSON string into a JSON object.
        payload = json.loads(event_body)
        # This is basically an array. The inner array contains the
        # parameter values passed to the function.
        rows = payload["data"]

        # Create a KMS client
        kms = boto3.client('kms', region_name='us-west-2')

        # Specify your asymmetric and symmetric key ARN or Alias
        asymmetric_key_id = "your-asymmetric-key-arn-or-alias"
        symmetric_key_id = "your-symmetric-key-arn-or-alias"

        # For each input row in the JSON object...
        for row in rows:
            # Read the input parameter's values.
            input_value_1 = row[0]
            input_value_2 = row[1]

            if input_value_1 == 1 and input_value_2 == "asymmetric":
                # Fetch the asymmetric key details
                key_metadata = kms.describe_key(KeyId=asymmetric_key_id)['KeyMetadata']
                json_compatible_string_to_return = json.dumps(key_metadata)

            elif input_value_1 == 2 and input_value_2 == "symmetric":
                # Fetch the symmetric key details
                key_metadata = kms.describe_key(KeyId=symmetric_key_id)['KeyMetadata']
                json_compatible_string_to_return = json.dumps(key_metadata)

            else:
                status_code = 400
                json_compatible_string_to_return = "Invalid input values."

    except Exception as err:
        # 400 implies some type of error.
        status_code = 400
        # Tell caller what this function could not handle.
        json_compatible_string_to_return = str(err)

    # Return the return value and HTTP status code.
    return {
        'statusCode': status_code,
        'body': json_compatible_string_to_return
    }
