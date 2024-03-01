import snowpark
from snowpark.sql import functions as sf
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
import base64
import os
# Encryption key (ensure consistent key management in production)
key = AESGCM.generate_key(bit_length=128)
def encrypt_data_base64(data):
   # Encrypt data using AES-GCM encryption
   aesgcm = AESGCM(key)
   nonce = os.urandom(12)
   encrypted = aesgcm.encrypt(nonce, data.encode('utf-8'), None)
   return base64.b64encode(nonce + encrypted).decode()
def decrypt_data_base64(data):
   # Decrypt data using AES-GCM encryption
   decoded = base64.b64decode(data.encode())
   aesgcm = AESGCM(key)
   nonce = decoded[:12]
   ciphertext = decoded[12:]
   return aesgcm.decrypt(nonce, ciphertext, None).decode('utf-8')
def main(session: snowpark.Session):
   # Retrieve data from the 'EMPLOYEE' table
   table_name = 'EMPLOYEE'
   dataframe = session.table(table_name)
   # Decrypt specified columns
   cols_to_decrypt = ['SSN', 'Salary']
   decrypted_cols = [decrypt_data_base64(col) for col in cols_to_decrypt]
   # Select all existing columns along with decrypted columns
   decrypted_dataframe = dataframe.select(dataframe.columns + decrypted_cols)
   return decrypted_dataframe
