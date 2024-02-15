from sqlalchemy import create_engine, text
from snowflake.sqlalchemy import URL
import pandas as pd
import logging

class SnowflakeDB:
    """This class provides methods to perform operations on a Snowflake database."""

    def __init__(self, user, password, account, warehouse, database):
        """
        Initialize connection parameters for the Snowflake database.

        Parameters:
        user (str): The Snowflake username.
        password (str): The password for the Snowflake user.
        account (str): The Snowflake account in the format 'xy12345.west-europe.azure'.
        database (str): The name of the Snowflake database.
        warehouse (str): The name of the Snowflake warehouse.

        Example Usage:
        db = SnowflakeDB('<username>', '<password>', '<account>', '<warehouse>', '<database>')
        """
        self.user = user
        self.password = password
        self.account = account
        self.warehouse = warehouse
        self.database = database
        self.logger = logging.getLogger(__name__)
        self.logger.info("Database connection parameters initialization complete.")

    def get_engine(self):
        """
        Establish and return an engine to the Snowflake database.

        Returns:
        sqlalchemy.engine.Engine: SQLAlchemy engine instance.

        Example Usage:
        engine = db.get_engine()
        """
        connection_string = URL(
            user=self.user,
            password=self.password,
            account=self.account,
            warehouse=self.warehouse,
            database=self.database
        )
        self.logger.info("Snowflake Database connection established.")
        return create_engine(connection_string)

    def select_data(self, schema_name: str, table_name: str, where_conditions: dict = None) -> list:
        """
        Fetches and returns rows from the specified table that match the 'where' condition.

        Parameters:
        schema_name (str): Name of the schema in which the table is located.
        table_name (str): Name of the table from which to fetch data.
        where_conditions (dict): Dictionary with column names as keys and corresponding values as conditions to select rows.

        Returns:
        list: List of tuples representing the fetched data, with each tuple representing a row in the result.

        Example Usage Without WHERE condition:
        results = db.select_data("MY_SCHEMA", "MY_TABLE")

        Example Usage WITH WHERE condition:
        where_conditions = {
            "COL1": "value1",
            "COL2": "value2",
            "COL3": "value3"
        }
        results = db.select_data("MY_SCHEMA", "MY_TABLE", where_conditions)
        """
        self.logger.info(f"Fetching data from Snowflake table: {table_name}")
        query = f'SELECT * FROM "{schema_name}"."{table_name}"'
        if where_conditions:
            conditions_str = ' AND '.join([f'"{k}" = \'{v}\'' for k, v in where_conditions.items()])
            query += f' WHERE {conditions_str}'
        with self.get_engine().connect() as connection:
            result = connection.execute(text(query))
        rows = result.fetchall()  # list of tuples
        self.logger.info(f"Fetched {len(rows)} rows from Snowflake table: {table_name}")
        return rows

    def insert_data(self, schema_name: str, table_name: str, data: list) -> None:
        """
        Inserts new rows into the specified table with provided data.

        Parameters:
        schema_name (str): Name of the schema in which the table is located.
        table_name (str): Name of the table to insert data into.
        data (list): List of dictionaries containing column names as keys and corresponding data as values.
                     Each dictionary represents a new row to be inserted.

        Example Usage:
        data = [
            {
                "COL1": "value1",
                "COL2": "value2",
                "COL3": "value3"
            },
            {
                "COL1": "value4",
                "COL2": "value5",
                "COL3": "value6"
            }
        ]
        db.insert_data("MY_SCHEMA", "MY_TABLE", data)
        """
        self.logger.info(f"Inserting data into Snowflake table: {table_name}")
        with self.get_engine().begin() as connection:
            for row in data:
                keys_list = ', '.join([f'"{k}"' for k in row.keys()])
                values_list = ', '.join([f"'{v}'" for v in row.values()])
                query = f'INSERT INTO "{schema_name}"."{table_name}" ({keys_list}) VALUES ({values_list})'
                connection.execute(text(query))
        self.logger.info(f"Inserted {len(data)} rows into Snowflake table: {table_name}")

    def update_data(self, schema_name: str, table_name: str, set_values: dict, where_conditions: dict) -> None:
        """
        Updates rows in the specified table that match the 'where' conditions.
        Columns to be updated and their new values are provided in the 'set_values' dictionary.

        Parameters:
        schema_name (str): Name of the schema in which the table is located.
        table_name (str): Name of the table to update.
        set_values (dict): Dictionary that contains column names as keys and the new values to update.
        where_conditions (dict): Dictionary with column names as keys and corresponding values as conditions to select rows.

        Example Usage:
        set_values = {
            "COL1": "newvalue1",
            "COL2": "newvalue2",
            "COL3": "newvalue3"
        }
        where_conditions = {
            "COL1": "value1",
            "COL2": "value2",
            "COL3": "value3"
        }
        db.update_data("MY_SCHEMA", "MY_TABLE", set_values, where_conditions)
        """
        self.logger.info(f"Updating the data in the Snowflake table: {table_name}")
        set_clause = ', '.join([f'"{k}" = \'{v}\'' for k, v in set_values.items()])
        where_clause = ' AND '.join([f'"{k}" = \'{v}\'' for k, v in where_conditions.items()])
        query = f'UPDATE "{schema_name}"."{table_name}" SET {set_clause} WHERE {where_clause}'
        with self.get_engine().begin() as connection:
            connection.execute(text(query))
        self.logger.info(f"Data updated in the Snowflake table: {table_name}")
