import snowflake.connector

class SnowflakeSync:
    def __init__(self, qa_conn_info, uat_conn_info):
        # Initialize with connection info for QA and UAT environments
        self.qa_conn_info = qa_conn_info
        self.uat_conn_info = uat_conn_info
        # Dictionaries to store object definitions
        self.qa_objects = {'databases': [], 'schemas': [], 'views': {}, 'procedures': {}}
        self.uat_objects = {'databases': [], 'schemas': [], 'views': {}, 'procedures': {}}

    def fetch_databases(self, conn_info, target_dict):
        # Fetch database names from Snowflake and store them in the target dictionary
        with snowflake.connector.connect(**conn_info) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("SHOW DATABASES")
                for row in cursor:
                    target_dict['databases'].append(row['name'])
            finally:
                cursor.close()

    def fetch_schemas(self, conn_info, target_dict):
        # Fetch schema names from Snowflake and store them in the target dictionary
        with snowflake.connector.connect(**conn_info) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("SHOW SCHEMAS")
                for row in cursor:
                    target_dict['schemas'].append(f"{row['database_name']}.{row['name']}")
            finally:
                cursor.close()

    def fetch_views(self, conn_info, target_dict):
        # Fetch view definitions from Snowflake and store them in the target dictionary
        with snowflake.connector.connect(**conn_info) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    SELECT TABLE_SCHEMA, TABLE_NAME, VIEW_DEFINITION
                    FROM INFORMATION_SCHEMA.VIEWS
                """)
                for schema, view_name, definition in cursor:
                    full_view_name = f"{schema}.{view_name}"
                    target_dict['views'][full_view_name] = definition.strip()
            finally:
                cursor.close()

    def fetch_stored_procs(self, conn_info, target_dict):
        # Fetch stored procedure definitions from Snowflake and store them in the target dictionary
        with snowflake.connector.connect(**conn_info) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    SELECT PROCEDURE_SCHEMA, PROCEDURE_NAME
                    FROM INFORMATION_SCHEMA.PROCEDURES
                """)
                for schema, proc_name in cursor:
                    full_proc_name = f"{schema}.{proc_name}"
                    # Use SHOW PROCEDURES to get the signature of the procedure
                    cursor.execute(f"SHOW PROCEDURES LIKE '{proc_name}' IN SCHEMA {schema}")
                    proc_result = cursor.fetchone()
                    if proc_result:
                        proc_signature = proc_result['arguments']
                        # Use GET_DDL to get the full definition of the procedure
                        ddl_result = conn.cursor().execute(f"SELECT GET_DDL('PROCEDURE', '{full_proc_name}({proc_signature})')").fetchone()
                        if ddl_result:
                            target_dict['procedures'][full_proc_name] = ddl_result[0].strip()
            finally:
                cursor.close()

    def compare_databases(self):
        # Compare databases and generate SQL for missing ones
        missing_in_qa = set(self.uat_objects['databases']) - set(self.qa_objects['databases'])
        sql_statements = [f"CREATE DATABASE {db};" for db in missing_in_qa]
        return sql_statements

    def compare_schemas(self):
        # Compare schemas and generate SQL for missing ones
        missing_in_qa = set(self.uat_objects['schemas']) - set(self.qa_objects['schemas'])
        sql_statements = [f"CREATE SCHEMA {schema};" for schema in missing_in_qa]
        return sql_statements

    def compare_views(self):
        # Compare views and generate SQL for missing or altered ones
        sql_statements = []
        for view, definition in self.uat_objects['views'].items():
            if view not in self.qa_objects['views']:
                # View is missing in QA
                sql_statements.append(f"CREATE OR REPLACE VIEW {view} AS {definition};")
            elif self.qa_objects['views'][view] != definition:
                # View definition has changed
                sql_statements.append(f"CREATE OR REPLACE VIEW {view} AS {definition};")
        return sql_statements

    def compare_stored_procs(self):
        # Compare stored procedures and generate SQL for missing or altered ones
        sql_statements = []
        for proc, definition in self.uat_objects['procedures'].items():
            if proc not in self.qa_objects['procedures']:
                # Stored procedure is missing in QA
                sql_statements.append(f"CREATE OR REPLACE PROCEDURE {proc} {definition};")
            elif self.qa_objects['procedures'][proc] != definition:
                # Stored procedure definition has changed
                sql_statements.append(f"CREATE OR REPLACE PROCEDURE {proc} {definition};")
        return sql_statements

    def compare_and_sync(self):
        # Fetch definitions from both environments
        self.fetch_databases(self.qa_conn_info, self.qa_objects)
        self.fetch_databases(self.uat_conn_info, self.uat_objects)
        self.fetch_schemas(self.qa_conn_info, self.qa_objects)
        self.fetch_schemas(self.uat_conn_info, self.uat_objects)
        self.fetch_views(self.qa_conn_info, self.qa_objects)
        self.fetch_views(self.uat_conn_info, self.uat_objects)
        self.fetch_stored_procs(self.qa_conn_info, self.qa_objects)
        self.fetch_stored_procs(self.uat_conn_info, self.uat_objects)

        # Perform comparison and generate SQL statements
        all_sql_statements = []
        all_sql_statements.extend(self.compare_databases())
        all_sql_statements.extend(self.compare_schemas())
        all_sql_statements.extend(self.compare_views())
        all_sql_statements.extend(self.compare_stored_procs())

        # Return the list of SQL statements to execute
        return all_sql_statements

# Example usage:
qa_conn_info = {
    'user': 'your_qa_user',
    'password': 'your_qa_password',
    'account': 'your_qa_account',
    # ... other connection parameters
}

uat_conn_info = {
    'user': 'your_uat_user',
    'password': 'your_uat_password',
    'account': 'your_uat_account',
    # ... other connection parameters
}

# Create an instance of the SnowflakeSync class
sync = SnowflakeSync(qa_conn_info, uat_conn_info)

# Call the compare_and_sync method to fetch definitions, compare, and generate SQL statements
sql_statements_to_execute = sync.compare_and_sync()

# Print or log the SQL statements to execute
for sql in sql_statements_to_execute:
    print(sql)
