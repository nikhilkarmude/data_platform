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

    def execute_sql_query(self, query: str) -> list:
        """
        Executes a SQL query on the Snowflake database.

        Parameters:
        query (str): A string containing a valid SQL query.

        Returns:
        A list of dictionaries, each dictionary represents a row of the result. Keys are column names, 
        values are data in respective columns for a particular row.

        Raises:
        ArgumentError: An error occurred when executing the SQL statement.

        Example Usage:
        results = db.execute_sql_query('SELECT * FROM MYSCHEMA.MYTABLE')
        for row in results:
            print(row)
        """
        try:
            with self.get_engine().connect() as connection:
                result = connection.execute(text(query))  # ResultProxy that acts like a Cursor
            self.logger.info(f"SQL query executed: {query}")
            rows = [dict(row) for row in result]
            return rows

        except Exception as e:
            self.logger.error(f"An error occurred: {e}")
            raise ArgumentError(f"An error occurred when executing the SQL statement: {query}. Error: {e}")


    def insert__bulk_data(self, schema_name: str, table_name: str, data: list) -> None:
        """
        Inserts new rows into the specified table with provided data.

        Parameters:
        schema_name (str): Name of the schema in which the table is located.
        table_name (str): Name of the table to insert data into.
        data (list): List of dictionaries containing column names as keys and corresponding data as values.
                    Each dictionary represents a row to be inserted.
        """
        self.logger.info(f"Inserting data into Snowflake table: {table_name}")
        keys_list = ', '.join([f'"{k}"' for k in data[0].keys()])
        rows_str_list = []
        for row in data:
            values_list = ', '.join([f"'{v}'" for v in row.values()])
            row_str = f'({values_list})'
            rows_str_list.append(row_str)
        all_rows_str = ', '.join(rows_str_list)
        query = f'INSERT INTO "{schema_name}"."{table_name}" ({keys_list}) VALUES {all_rows_str}'
        with self.get_engine().begin() as conn:
            conn.execute(text(query))
        self.logger.info(f"Inserted {len(data)} rows into Snowflake table: {table_name}")


    def call_stored_procedure(self, schema_name: str, procedure_name: str, args: str) -> None:
        """
        Calls a stored procedure with the provided arguments.

        Args:
            schema_name (str): Name of the schema in which the procedure is located.
            procedure_name (str): Name of the stored procedure.
            args (str): Comma-separated arguments to pass to the stored procedure.

        Example usage:
            db.call_stored_procedure('MYSCHEMA', 'MYPROCEDURE', 'arg1, arg2, arg3')
        """
        self.logger.info(f"Calling stored procedure: {procedure_name}")
        query = f'CALL "{schema_name}"."{procedure_name}"({args})'
        with self.get_engine().begin() as connection:
            connection.execute(text(query))
        self.logger.info(f"Executed the stored procedure: {procedure_name}")



        def delete_data(self, schema_name: str, table_name: str, where_conditions: dict) -> None:
    """
    Deletes rows in the specified table that match the 'where' conditions.

    Parameters:
    schema_name (str): Name of the schema in which the table is located.
    table_name (str): Name of the table from which to delete data.
    where_conditions (dict): Dictionary with column names as keys and corresponding values as conditions to select rows for deletion.

    Example Usage:
    where_conditions = {
        "COL1": "value1",
        "COL2": "value2",
        "COL3": "value3"
    }
    db.delete_data("MY_SCHEMA", "MY_TABLE", where_conditions)
    """
    self.logger.info(f"Deleting data from Snowflake table: {table_name}")
    where_clause = ' AND '.join([f'"{k}" = \'{v}\'' for k, v in where_conditions.items()])
    query = f'DELETE FROM "{schema_name}"."{table_name}" WHERE {where_clause}'
    with self.get_engine().begin() as connection:
        connection.execute(text(query))
    self.logger.info(f"Data deleted from the Snowflake table: {table_name}")
    

def truncate_table(self, schema_name: str, table_name: str) -> None:
    """
    Truncates the specified table in the Snowflake database.

    Parameters:
    schema_name (str): Name of the schema in which the table is located.
    table_name (str): Name of the table to truncate.

    Example Usage:
    db.truncate_table("MY_SCHEMA", "MY_TABLE")
    """
    self.logger.info(f"Truncating Snowflake table: {table_name}")
    query = f'TRUNCATE TABLE "{schema_name}"."{table_name}"'
    with self.get_engine().begin() as connection:
        connection.execute(text(query))
    self.logger.info(f"Truncated the Snowflake table: {table_name}")

def execute_stored_proc(self, schema_name: str, stored_proc_name: str, params: list = None) -> None:
    """
    Executes a stored procedure in the Snowflake database.

    Parameters:
    schema_name (str): Name of the schema in which the stored procedure is located.
    stored_proc_name (str): Name of the stored procedure to execute.
    params (list): List of parameters to pass to the stored procedure.

    Example Usage:
    db.execute_stored_proc("MY_SCHEMA", "MY_STORED_PROCEDURE", ["param1", "param2", "param3"])
    """
    self.logger.info(f"Executing stored procedure: {stored_proc_name}")
    query = f'CALL "{schema_name}"."{stored_proc_name}"({", ".join(["?" for _ in params])})'
    with self.get_engine().begin() as connection:
        connection.execute(text(query), params)
    self.logger.info(f"Executed the stored procedure: {stored_proc_name}")

