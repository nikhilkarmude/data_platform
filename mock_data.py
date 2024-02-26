import psycopg2
from faker import Faker

# Initialize the Faker library
fake = Faker()

# Define an empty list to hold our fake employee data
data = []

# Generate 10000 employee records 
for _ in range(10000):
    data.append([fake.unique.random_number(digits=5), 
                fake.first_name(), 
                fake.last_name(), 
                fake.job(), 
                fake.email(), 
                fake.ssn(),  # For Social Security Number
                fake.random_int(min=30000, max=120000),  # For Salary
                fake.date_between(start_date='-30y', end_date='today')])

# Establish a connection with PostgreSQL database
conn = psycopg2.connect(
    dbname="your_db_name",
    user="your_username",
    password="your_password",
    host="localhost",
    port="5432"
)

cur = conn.cursor()

# SQL command to insert dataframe into PostgreSQL table
insert_query_template = """
    INSERT INTO your_table (emp_id, first_name, last_name, occupation, email, ssn, salary, dob)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

# Use executemany to insert all rows in one go
cur.executemany(insert_query_template, data)

# Commit changes and close
conn.commit()
cur.close()
conn.close()
