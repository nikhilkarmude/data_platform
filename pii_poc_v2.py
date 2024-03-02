import pandas as pd
from sqlalchemy import create_engine
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
import boto3
import json
import base64

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
        self.engine = None

    def connect(self):
        try:
            pg_conn_str = (
                f"postgresql://{self.secrets['POSTGRES_USERNAME']}:{self.secrets['POSTGRES_PASSWORD']}@"
                f"{self.secrets['POSTGRES_HOST']}:{self.secrets['POSTGRES_PORT']}/{self.secrets['POSTGRES_DATABASE']}"
            )
            self.engine = create_engine(pg_conn_str)
            return self.engine
        except Exception as e:
            print(f"Error: Could not make connection to the Postgres database: {e}")
        return None

    def read_chunks(self, query, chunksize=10000):
        return pd.read_sql_query(query, self.engine, chunksize=chunksize)

class Cryptographer:
    def __init__(self, key):
        self.key = base64.b64decode(key)

    def encrypt_data_base64(self, data):
        aesgcm = AESGCM(self.key)
        nonce = os.urandom(12)
        encrypted = aesgcm.encrypt(nonce, data.encode('utf-8'), None)
        return base64.b64encode(nonce + encrypted).decode()

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

def main():
    sm = SecretManager('postgres_secrets', 'snowflake_secrets', 'encryption_key_secrets')
    postgres_secrets = sm.get_postgres_secret()
    snowflake_secrets = sm.get_snowflake_secret()
    encryption_key_secret = sm.get_encryption_key_secret()

    cryptographer = Cryptographer(encryption_key_secret)

    pc = PostgresConnector(postgres_secrets)
    pc.connect()

    query = 'SELECT * FROM my_table;'
    chunks = pc.read_chunks(query)

    sc = SnowflakeDB(snowflake_secrets['SNOWFLAKE_USER'],
                     snowflake_secrets['SNOWFLAKE_PASSWORD'],
                     snowflake_secrets['SNOWFLAKE_ACCOUNT'],
                     snowflake_secrets['SNOWFLAKE_WAREHOUSE'],
                     snowflake_secrets['SNOWFLAKE_DATABASE'])

    engine = sc.get_engine()

    for df_chunk in chunks:
        df_chunk['fssn'] = df_chunk['fssn'].apply(cryptographer.encrypt_data_base64)
        df_chunk['salary'] = df_chunk['salary'].apply(cryptographer.encrypt_data_base64)

        df_chunk.to_sql('table_name', con=engine, schema='schema_name', 
                        if_exists='append', index=False, method='multi')

if __name__ == '__main__':
    main()
