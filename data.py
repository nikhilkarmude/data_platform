from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from snowflake.sqlalchemy import URL        
from sqlalchemy import create_engine
import pandas as pd

# initialise GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)

# Original DynamicFrame
dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
     database = "your_database",
     table_name = "your_table_name",
     transformation_ctx = "datasource0"
)

# create a new DynamicFrame by filtering the original one
dynamic_frame2 = dynamic_frame.filter(predicate=lambda x: x["column_name"] == "value")

# create another DynamicFrame by selecting specific fields
dynamic_frame3 = dynamic_frame.select_fields(["column1", "column2", "column3"])

# Convert the dynamic frames to dataframes
df2 = dynamic_frame2.toDF()
df3 = dynamic_frame3.toDF()

# Convert spark dataframes to pandas dataframes
pandas_df2 = df2.toPandas()
pandas_df3 = df3.toPandas()

# Connection details for snowflake
engine = create_engine(URL(
    user="username",
    password="password",
    account="account_url",
    database="database_name",
    schema="schema_name",
    warehouse="warehouse_name"
))

# Chunk size
chunk_size = 10000

# Write the pandas dataframes back to snowflake in chunks
for pandas_df in [pandas_df2, pandas_df3]:
    for i in range(0, len(pandas_df), chunk_size):
        pandas_df[i:i + chunk_size].to_sql('your_table_name', engine, if_exists='append', index=False)
        print(f"Chunk {i//chunk_size + 1} loaded.")
