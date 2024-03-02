import os
import boto3
import psycopg2
import json
import base64

from sqlalchemy import create_engine, text
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from snowflake.sqlalchemy import URL


class SecretManager:
    def __init__(self, postgres_secret_name, snowflake_secret_name, encryption_key_secret_name, region_name='us-west-2'):
        self.postgres_secret_name = postgres_secret_name
        self.snowflake_secret_name = snowflake_secret_name
        self.encryption_key_secret_name = encryption_key_secret_name
        self.region_name = region_name

    def get_secret(self, secret_name):
        session = boto3.session.Session()
        client = session.client(service_name='secretsmanager', region_name=self.region_name)
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)

        return json.loads(get_secret_value_response['SecretString'])

    def get_postgres_secret(self):
        return self.get_secret(self.postgres_secret_name)

    def get_snowflake_secret(self):
        return self.get_secret(self.snowflake_secret_name)

    def get_encryption_key_secret(self):
        return self.get_secret(self.encryption_key_secret_name)


class PostgresConnector:
    def __init__(self, secrets):
        self.secrets = secrets
        self.conn = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                database=self.secrets['POSTGRES_DATABASE'],
                user=self.secrets['POSTGRES_USERNAME'],
                password=self.secrets['POSTGRES_PASSWORD'],
                host=self.secrets['POSTGRES_HOST'],
                port=self.secrets['POSTGRES_PORT']
            )
            
            return self.conn
        except psycopg2.Error as e:
            print(f"Error: Could not make connection to the Postgres database")
            print(e)
            
        return None

    def read_chunks(self, query, chunk_size=10000):
        cur = self.conn.cursor()
        offset = 0
        
        while True:
            cur.execute(query.format(offset, chunk_size))
            chunk = cur.fetchall()
            
            if not chunk:
                break
            
            yield chunk
            
            offset += chunk_size


class Cryptographer:
    def __init__(self, key):
        self.key = base64.urlsafe_b64decode(key)

    def encrypt_data_base64(self, data):
        aesgcm = AESGCM(self.key)
        nonce = os.urandom(12)
        encrypted = aesgcm.encrypt(nonce, data.encode('utf-8'), None)

        return base64.b64encode(nonce + encrypted).decode()

    def decrypt_data_base64(self, data):
        decoded = base64.b64decode(data.encode())
        aesgcm = AESGCM(self.key)
        nonce = decoded[:12]
        ciphertext = decoded[12:]

        return aesgcm.decrypt(nonce, ciphertext, None).decode()


class SnowflakeDB:
    def __init__(self, user, password, account, warehouse, database):
        self.user = user
        self.password = password
        self.account = account
        self.warehouse = warehouse
        self.database = database

    def get_engine(self):
        connection_string = URL(
            user=self.user,
            password=self.password,
            account=self.account,
            warehouse=self.warehouse,
            database=self.database
        )

        return create_engine(connection_string)

    def insert_data(self, schema_name: str, table_name: str, data: list) -> None:
        keys_list = ', '.join([f'"{k}"' for k in data[0].keys()])
        for record in data:
            values_list = ', '.join([f"'{v}'" for v in record.values()])
            query = f'INSERT INTO "{schema_name}"."{table_name}" ({keys_list}) VALUES ({values_list})'
            
            with self.get_engine().begin() as conn:
                conn.execute(text(query))


def main():
    sm = SecretManager('postgres_secrets', 'snowflake_secrets', 'encryption_key_secrets')

    postgres_secrets = sm.get_postgres_secret()
    snowflake_secrets = sm.get_snowflake_secret()
    encryption_key_secret = sm.get_encryption_key_secret()

    cryptographer = Cryptographer(encryption_key_secret)

    pc = PostgresConnector(postgres_secrets)
    conn = pc.connect()
    query = 'SELECT * FROM my_table ORDER BY id OFFSET {} LIMIT {};'
    chunks = pc.read_chunks(query)

    sc = SnowflakeDB(snowflake_secrets['SNOWFLAKE_USER'], 
                     snowflake_secrets['SNOWFLAKE_PASSWORD'], 
                     snowflake_secrets['SNOWFLAKE_ACCOUNT'], 
                     snowflake_secrets['SNOWFLAKE_WAREHOUSE'], 
                     snowflake_secrets['SNOWFLAKE_DATABASE'])

    for chunk in chunks:
        for row in chunk:
            # Encrypt the data according to your requirement
            row['fssn'] = cryptographer.encrypt_data_base64(row['fssn'])
            row['salary'] = cryptographer.encrypt_data_base64(row['salary'])

        sc.insert_data("YOUR_SCHEMA", "YOUR_TABLE", chunk)

    conn.close()


if __name__ == '__main__':
    main()



#
    The `AESGCM.generate_key(bit_length=256)` function from the `cryptography` library generates a random 256-bit key for AESGCM.

However, the generated key is a bytes object. If you want to store it in Secrets Manager, you should first convert it to a base64 string:

```python
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
import base64

key = AESGCM.generate_key(bit_length=256)
base64_key = base64.b64encode(key).decode('utf-8')
```

And when you want to use it, get the secret as a string and then convert the base64 string back to bytes:

```python
# Assuming encryption_key_secret is a string retrieved from Secrets Manager
key = base64.b64decode(encryption_key_secret)
cryptographer = Cryptographer(key)
```

Ensure your encryption key in AWS Secrets Manager is a base64 encoded string formed as shown above. This is the usual convention of storing byte data in string format. You should not store and retrieve the bytes key directly as string. If you did so, you may need to update your secret to base64 format as shown above.