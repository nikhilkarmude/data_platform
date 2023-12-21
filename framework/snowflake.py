import os, sys
# below lines are added so that we can run this file as - python ingestion.py
# without facing module not found error.
# If these lines are removed, then below import statements need to be modified.
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)


import snowflake.connector
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SnowflakeDatabase:

    def __init__(self, user, password, account, warehouse, database, schema):
        """Initialize connection parameters and connect to the Snowflake DB."""
        self.user = user
        self.password = password
        self.account = account
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.connection = self.connect()

    def connect(self):
        """Establish and return a connection with the Snowflake DB."""
        logger.info("Connecting to Snowflake database")
        return snowflake.connector.connect(
            user=self.user,
            password=self.password,
            account=self.account,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema)

    def select(self, query):
        """Perform a SELECT query and return results.
        Parameters:
            query (str): The SQL query to execute.
        Returns:
            list: The query result as a list of tuples.
        """
        logger.info("Fetching data from database")
        cur = self.connection.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        cur.close()
        return rows

    def insert(self, query, data):
        """Perform an INSERT operation.
        Parameters:
            query (str): The SQL query to execute.
            data (list): The data to be inserted.
        """
        logger.info("Inserting data into database")
        cur = self.connection.cursor()
        cur.execute(query, data)
        self.connection.commit()
        cur.close()

    def update(self, query):
        """Perform an UPDATE operation.
        Parameters:
            query (str): The SQL query to execute.
        """
        logger.info("Updating data in database")
        cur = self.connection.cursor()
        cur.execute(query)
        self.connection.commit()
        cur.close()

    def delete(self, query):
        """Perform a DELETE operation.
        Parameters:
            query (str): The SQL query to execute.
        """
        logger.info("Deleting data from database")
        cur = self.connection.cursor()
        cur.execute(query)
        self.connection.commit()
        cur.close()

    def execute_procedure(self, proc_name, *args):
        """Execute a stored procedure with given arguments.
        Parameters:
            proc_name (str): The name of the stored procedure.
            *args: Variable length argument list for stored procedure parameters.
        """
        logger.info("Running stored procedure")
        cur = self.connection.cursor()
        call = "CALL {}({})".format(proc_name, ','.join(['%s'] * len(args)))
        cur.execute(call, tuple(args))
        self.connection.commit()
        cur.close()

    def close(self):
        """Close the connection with the Snowflake DB."""
        logger.info("Closing connection to database")
        self.connection.close()



# db = SnowflakeDatabase(user='username', password='password', account='account_url', warehouse='warehouse', database='database', schema='public')

# # SELECT
# rows = db.select("SELECT * FROM table")
# print(rows)

# # Insert
# db.insert("INSERT INTO table (column) VALUES (%s)", ['value'])

# # Update
# db.update("UPDATE table SET column = 'new_value' WHERE column = 'old_value'")

# # Delete
# db.delete("DELETE FROM table WHERE column = 'value'")

# # Stored Procedure
# db.execute_procedure('stored_proc', 'param1', 'param2', 'param3')

# db.close()
