from cryptography.hazmat.primitives.ciphers.aead import AESGCM
import pandas as pd
import base64, os

key = AESGCM.generate_key(bit_length=128)
print(f"Encryption key {key}")

# Function to encrypt data and return base64
def encrypt_data_base64(data):
    aesgcm = AESGCM(key)
    nonce = os.urandom(12)
    encrypted = aesgcm.encrypt(nonce, data.encode('utf-8'), None)
    return base64.b64encode(nonce + encrypted).decode()  # combine nonce with encrypted data

# Function to decrypt data
def decrypt_data_base64(data):
    decoded = base64.b64decode(data.encode())  # Decode base64 to binary
    aesgcm = AESGCM(key)
    nonce = decoded[:12]   # First 12 byte is nonce
    ciphertext = decoded[12:]  # Rest is the ciphertext
    return aesgcm.decrypt(nonce, ciphertext, None).decode('utf-8')

def main(session: snowpark.session):
    table_name = 'EMPLOYEE'
    dataframe = session.table(table_name)
    results = dataframe.collect()
    pandas_df = pd.Dataframe(results, columns = dataframe.columns)

    cols_to_decrypt = ['SSN', 'Salary']

    for col in cols_to_decrypt:
        pandas_df[col] = pandas_df[col].apply(decrypt_data_base64)

    snowflake_df = session.create_dataframe(pandas_df)

    return snowflake_df


temp_text = "Hello, World!"
print(f"Original Text: {temp_text}")

temp_encrypted = encrypt_data_base64(temp_text)
print(f"Encrypted Text: {temp_encrypted}")

temp_decrypted = decrypt_data_base64(temp_encrypted)
print(f"Decrypted Text: {temp_decrypted}")

