import os, sys
# below lines are added so that we can run this file as - python ingestion.py
# without facing module not found error.
# If these lines are removed, then below import statements need to be modified.
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

from sqlalchemy import create_engine, MetaData, Table, delete, insert, update
from sqlalchemy.orm import Session
from sqlalchemy.sql.expression import select
import logging

logger = logging.getLogger(__name__)

class ABC:
    """This class provides methods to perform operations on a PostgreSQL database."""

    def __init__(self):
        """Initialize connection parameters for the PostgreSQL database."""
        self.user = "postgres"
        self.password = "admin"
        self.host = "localhost"
        self.port = "5432"
        self.database = "cdp"
        logger.info("Database connection parameters initialized")

    def get_engine(self):
        """Establish and return an engine to the PostgreSQL database."""
        connection_string = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        logger.info("Database connection established.")
        return create_engine(connection_string)

    def get_table(self, table_name: str, schema_name: str) -> Table:
        """Fetches the specified table from the database with auto loading its columns.

        Parameters:
        table_name (str): Name of the table to fetch.
        schema_name (str): Name of the schema.

        Returns:
        sqlalchemy.Table: a reference to the table within SQLAlchemy.

        Example Usage:
        table = abc.get_table("SYSTEM_CONTROL_TBL", "public")
        """
        logger.info(f"Fetching table: {table_name}")
        metadata = MetaData()
        table = Table(
            table_name, metadata, autoload_with=self.get_engine(), schema=schema_name
        )
        logger.info(f"Fetched table: {table}")
        return table

    def get_table_data(
        self, table_name: str, schema_name: str, where_conditions: dict = None
    ) -> list:
        """Fetches and returns rows from the specified table that match the 'where' conditions.

        Parameters:
        table_name (str): Name of the table to fetch data from.
        schema_name (str): Name of the schema.
        where_conditions (dict, optional): Dictionary with column names as keys and corresponding values as conditions to select rows.

        Returns:
        list: list of result proxies containing fetched data.

        Example Usage Without WHERE condition:
        results = abc.get_table_data("SYSTEM_CONTROL_TBL", "public")

        Example Usage WITH WHERE condition:
        where_conditions = {
            "SYSTEM_NAME": "Test System",
            "SYSTEM_INSTANCE": "1"
        }
        results = abc.get_table_data("SYSTEM_CONTROL_TBL", "public", where_conditions)
        """
        logger.info(f"Fetching data from table: {table_name}")
        table = self.get_table(table_name, schema_name)
        stmt = select(table)
        if where_conditions:
            for col, value in where_conditions.items():
                stmt = stmt.where(getattr(table.c, col) == value)
        with Session(self.get_engine()) as session:
            results = session.execute(stmt).all()
        logger.info(f"Fetched {len(results)} rows from table: {table_name}")
        return results

    def insert_data(self, table_name: str, schema_name: str, data: list) -> None:
        """Inserts new rows into the specified table with provided data.

        Parameters:
        table_name (str): Name of the table to insert data into.
        schema_name (str): Name of the schema.
        data (list): List of dictionaries containing column names as keys and corresponding data as values.
                     Each dictionary represents a new row to be inserted.

        Example Usage:
        data = [
            {
                "SYSTEM_NAME": "Test System 1",
                "SYSTEM_SHORT_NAME": "TS1",
                "SYSTEM_DESCRIPTION": "This is a test system 1",
                "SYSTEM_INSTANCE": "1",
                "UP_DOWN_SYSTEM_FLAG": "U",
                "SYSTEM_OWNER": "Test Owner 1",
                "SYSTEM_CONTACT": "test1@example.com",
                "ACTIVE_FLAG": "Y",
                "CREATED_BY": "admin"
            },
            {
                "SYSTEM_NAME": "Test System 2",
                "SYSTEM_SHORT_NAME": "TS2",
                "SYSTEM_DESCRIPTION": "This is a test system 2",
                "SYSTEM_INSTANCE": "2",
                "UP_DOWN_SYSTEM_FLAG": "D",
                "SYSTEM_OWNER": "Test Owner 2",
                "SYSTEM_CONTACT": "test2@example.com",
                "ACTIVE_FLAG": "N",
                "CREATED_BY": "admin"
            }
        ]
        abc.insert_data("SYSTEM_CONTROL_TBL", data)
        """
        logger.info(f"Inserting data into table: {table_name}")
        table = self.get_table(table_name, schema_name)
        stmt = insert(table).values(data)
        with Session(self.get_engine()) as session:
            session.execute(stmt)
            session.commit()
        logger.info(f"Inserted {len(data)} rows into table: {table_name}")

    def update_data(
        self, table_name: str, schema_name: str, where_conditions: dict, my_val: dict
    ) -> None:
        """Updates existing rows matching the where conditions with new data in the specified table.

        Parameters:
        table_name (str): Name of the table for updating data.
        schema_name (str): Name of the schema.
        where_conditions (dict): Dictionary with column names as keys and corresponding values as conditions to select rows for updating.
        my_val (dict): Dictionary with column names as keys and new data as values to update in the selected rows.

        Example Usage:
        where_conditions = {
            "SYSTEM_NAME": "Test System",
            "SYSTEM_INSTANCE": "1"
        }
        new_values = {
            "SYSTEM_DESCRIPTION": "This is an updated test system",
            "UPDATED_BY": "admin"
        }
        abc.update_data("SYSTEM_CONTROL_TBL", where_conditions, new_values)
        """
        logger.info(f"Updating data in table: {table_name}")
        table = self.get_table(table_name, schema_name)
        stmt = update(table).values(my_val)
        for col, value in where_conditions.items():
            stmt = stmt.where(getattr(table.c, col) == value)
        with Session(self.get_engine()) as session:
            session.execute(stmt)
            session.commit()
        logger.info("Data updated in table: {table_name}")

    def delete_data(
        self, table_name: str, schema_name: str, where_conditions: dict
    ) -> None:
        """Deletes rows from the specified table matching the where conditions.

        Parameters:
        table_name (str): Name of the table to delete data from.
        schema_name (str): Name of the schema.
        where_conditions (dict): Dictionary with column names as keys and corresponding values as conditions to select rows for deletion.

        Example Usage:
        where_conditions = {
            "SYSTEM_NAME": "Test System",
            "SYSTEM_INSTANCE": "1"
        }
        abc.delete_data("SYSTEM_CONTROL_TBL", where_conditions)
        """
        logger.info(f"Deleting data from table: {table_name}")
        table = self.get_table(table_name, schema_name)
        stmt = delete(table)
        for col, value in where_conditions.items():
            stmt = stmt.where(getattr(table.c, col) == value)
        with Session(self.get_engine()) as session:
            result = session.execute(stmt)
            session.commit()
        logger.info(f"Deleted {result.rowcount} rows from table: {table_name}")


# # Instantiate the ABC class
# abc = ABC()

# # Specify schema name
# schema_name = "public"

# # 1. INSERT:
# # To insert multiple rows into the "SYSTEM_CONTROL_TBL":
# data = [
#     {
#         "SYSTEM_NAME": "Test System",
#         "SYSTEM_SHORT_NAME": "TS",
#         "SYSTEM_DESCRIPTION": "This is a test system",
#         "SYSTEM_INSTANCE": "1",
#         "UP_DOWN_SYSTEM_FLAG": "U",
#         "SYSTEM_OWNER": "Test Owner",
#         "SYSTEM_CONTACT": "test@example.com",
#         "ACTIVE_FLAG": "Y",
#         "CREATED_BY": "admin",
#     },
#     {
#         "SYSTEM_NAME": "Test System 2",
#         "SYSTEM_SHORT_NAME": "TS2",
#         "SYSTEM_DESCRIPTION": "This is a second test system",
#         "SYSTEM_INSTANCE": "2",
#         "UP_DOWN_SYSTEM_FLAG": "D",
#         "SYSTEM_OWNER": "Test Owner 2",
#         "SYSTEM_CONTACT": "test2@example.com",
#         "ACTIVE_FLAG": "N",
#         "CREATED_BY": "admin",
#     },
# ]
# abc.insert_data(table_name="SYSTEM_CONTROL_TBL", schema_name=schema_name, data=data)

# # 2. SELECT:
# # To fetch and print all data from the "SYSTEM_CONTROL_TBL":
# results = abc.get_table_data(table_name="SYSTEM_CONTROL_TBL", schema_name=schema_name)
# for row in results:
#     print(row)

# # 3. SELECT with WHERE condition
# # To fetch and print data matching conditions from the "SYSTEM_CONTROL_TBL":
# where_conditions = {"SYSTEM_NAME": "Test System", "SYSTEM_INSTANCE": "1"}
# results = abc.get_table_data(
#     table_name="SYSTEM_CONTROL_TBL",
#     schema_name=schema_name,
#     where_conditions=where_conditions,
# )
# for row in results:
#     print(row)

# # 4. UPDATE:
# # To update data in the "SYSTEM_CONTROL_TBL":
# where_conditions = {"SYSTEM_NAME": "Test System", "SYSTEM_INSTANCE": "1"}
# new_values = {
#     "SYSTEM_DESCRIPTION": "This is an updated test system",
#     "UPDATED_BY": "admin",
# }
# abc.update_data(
#     table_name="SYSTEM_CONTROL_TBL",
#     schema_name=schema_name,
#     where_conditions=where_conditions,
#     my_val=new_values,
# )

# # 5. SELECT After Update:
# # To fetch and print all data from the "SYSTEM_CONTROL_TBL":
# results = abc.get_table_data(table_name="SYSTEM_CONTROL_TBL", schema_name=schema_name)
# for row in results:
#     print(row)

# # 6. DELETE:
# # To delete data from the "SYSTEM_CONTROL_TBL":
# where_conditions = {"SYSTEM_NAME": "Test System", "SYSTEM_INSTANCE": "1"}
# abc.delete_data(
#     table_name="SYSTEM_CONTROL_TBL",
#     schema_name=schema_name,
#     where_conditions=where_conditions,
# )

# # 7. SELECT After Delete:
# # To fetch and print all data from the "SYSTEM_CONTROL_TBL":
# results = abc.get_table_data(table_name="SYSTEM_CONTROL_TBL", schema_name=schema_name)
# for row in results:
#     print(row)

import subprocess
self.dump_file = "database_dump.sql"
def dump_database(self):
    try:
        process = subprocess.Popen(
            ['pg_dump', 
                '--dbname=postgresql://{}:{}@{}:{}/{}'.format(self.user, self.password, self.host, self.port, self.database_1), 
                '-f', self.dump_file],
            stdout=subprocess.PIPE
        )
        output = process.communicate()[0]
    except Exception as e:
        print("An error occurred while taking database dump: ", str(e))