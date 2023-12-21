from aws_helper import AWSHelper
import logging
import base64

logging.basicConfig(level=logging.INFO)

class KMSHelper:

    def __init__(self, profile=None):
        """Initialize AWS KMS client
        Parameters: 
        profile (str): The AWS CLI profile name. Default is None.
                       'None' will use the default AWS CLI profile.

        Usage:
        kms_helper = KMSHelper(profile='my-profile')
        """
        logging.info('Initializing KMS client...')
        aws_helper = AWSHelper()
        kms_session = aws_helper.create_boto3_session(aws_profile=profile)
        self.kms_client = kms_session.client('kms')
        logging.info('KMS client initialized.')

    def create_key(self, description, key_usage):
        """Create a new KMS key.
        Parameters: 
        description (str): A description for the key.
        key_usage (str): The intended use of the key. For example, ENCRYPT_DECRYPT or SIGN_VERIFY.

        Returns:
        str: The AWS Key Management Service (AWS KMS) key identifier (key ID) of the customer master key (CMK).

        Usage:
        key_id = kms_helper.create_key('my-description', 'ENCRYPT_DECRYPT')
        """
        response = self.kms_client.create_key(
            Description=description,
            KeyUsage=key_usage
        )
        return response['KeyMetadata']['KeyId']

    def list_keys(self):
        """List all KMS keys.

        Returns:
        list: A list of keys.

        Usage:
        all_keys = kms_helper.list_keys()
        """
        response = self.kms_client.list_keys()
        return response['Keys']

    def encrypt(self, key_id, plaintext):
        """Encrypt plaintext using a given key.
        Parameters:
        key_id (str): The ID of the key to use for encryption.
        plaintext (str): The text to encrypt.

        Returns:
        str: The encrypted text, base64-encoded for readability and consistency.

        Usage:
        encrypted_text = kms_helper.encrypt('my-key-id', 'Hello, world!')
        """
        response = self.kms_client.encrypt(
            KeyId=key_id,
            Plaintext=plaintext.encode('utf-8')
        )
        ciphertext = response['CiphertextBlob']
        return base64.b64encode(ciphertext).decode()

    def decrypt(self, key_id, ciphertext):
        """Decrypt ciphertext using a given key.
        Parameters:
        key_id (str): The ID of the key to use for decryption.
        ciphertext (str): The text to decrypt (must be base64-encoded).

        Returns:
        str: The decrypted text.

        Usage:
        decrypted_text = kms_helper.decrypt('my-key-id', 'QAFK...N7A=')
        """
        ciphertext_blob = base64.b64decode(ciphertext)
        response = self.kms_client.decrypt(
            KeyId=key_id,
            CiphertextBlob=ciphertext_blob
        )
        return response['Plaintext'].decode()

# # Usage
# kms_helper = KMSHelper(profile='default')

# # Create a key
# key_id = kms_helper.create_key("my_cmk", "ENCRYPT_DECRYPT")
# print("Key id: ", key_id)

# # Print list of keys
# print("All Keys: ", kms_helper.list_keys())

# # Encryption
# message = "Hello, World!"
# ciphertext = kms_helper.encrypt(key_id, message)
# print("Encrypted message: ", ciphertext)

# # Decryption
# plaintext = kms_helper.decrypt(key_id, ciphertext)
# print("Decrypted message: ", plaintext)
