from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import ApplyMapping
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, FloatType

class DataProcessor:

    def __init__(self):
        self.sc = SparkContext()
        self.glueContext = GlueContext(self.sc)
        self.udf_dict = {
            "transformation_1": udf(self.custom_transformation_1, StringType()),
            "transformation_2": udf(self.custom_transformation_2, FloatType())
        }

    @staticmethod
    def custom_transformation_1(input_string):
        return input_string + '_custom_transform'

    @staticmethod
    def custom_transformation_2(input_float):
        return input_float * 2.0

    def process_data(self, source):
        jdbc_url = f"jdbc:snowflake://{source['account']}.snowflakecomputing.com/"
        connection_options = {
            "url": jdbc_url,
            "dbtable": source["table"],
            "database": source["database"],
            "warehouse": source["warehouse"],
            "schema": "public",
            "postactions": "SET",
            "user": source["user"],
            "password": source["password"],
        }

        dynamic_frame = self.glueContext.create_dynamic_frame.from_options(
            connection_type="snowflake",
            connection_options=connection_options)

        for mapping in source["mappings"]:
            dynamic_frame = ApplyMapping.apply(frame=dynamic_frame, mappings=mapping)

        df = dynamic_frame.toDF()

        for column, func in source["transformations"].items():
            df = df.withColumn(column, self.udf_dict[func](df[column]))

        dynamic_frame_transformed = DynamicFrame.fromDF(df, self.glueContext, "dynamic_frame_transformed")

        connection_options["schema"] = source["output_schema"]
        connection_options["dbtable"] = source["output_table"]
        self.glueContext.write_dynamic_frame.from_options(
            frame=dynamic_frame_transformed,
            connection_type="snowflake",
            connection_options=connection_options)


if __name__ == "__main__":
    sources = [{
        "account": "<account>",
        "table": "<table>",
        "database": "<database>",
        "warehouse": "<warehouse>",
        "user": "<user>",
        "password": "<password>",
        "mappings": [("column1", "string", "column1", "string"), 
                     ("column2", "int", "column2", "float")],
        "transformations": {"column1": "transformation_1", "column2": "transformation_2"},
        "output_schema": "<output_schema>",
        "output_table": "<output_table>"
    }]

    processor = DataProcessor()
    for source in sources:
        processor.process_data(source)
