import unittest
from test import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
def test_etl(self):
    #1. Prepare an input data frame that mimics our source data.
    input_schema = StructType([
                   StructField('cookTime', IntegerType()),
                   StructField('datePublished', StringType()),
                   StructField('description', StringType()),
                   StructField('image', StringType()),
                   StructField('ingredients', StringType()),
                   StructField('name', StringType()),
                   StructField('prepTime', IntegerType()),
                   StructField('recipeYield', StringType()),
                   StructField('url', StringType()),
                   StructField('TotalCookTime', IntegerType()),
                   StructField('TimeinMin', StringType()),
                   StructField('Difficulty', StringType())])
    input_data = [(1800, "2010-11-23", "Important note:", "http://static.the.","2 Tablespoons But..","French Onion Soup",1200,"8","http://thepioneer",3000,"50","Medium" ),
              (300, "2010-11-24", "Important note:", "http://static.the.","2 Tablespoons But..","French Onion Soup",900,"8","http://thepioneer",1200,"20","Easy" ),
                          (900, "2010-11-24", "Important note:", "http://static.the.","2 Tablespoons But..","French Onion Soup",7200,"8","http://thepioneer",8100,"135","Hard" )]
    input_df = self.spark.createDataFrame(data=input_data, schema=input_schema)
    #2. Prepare an expected data frame which is the output that we expect.
    expected_schema = StructType([
            StructField('Difficulty', StringType(), True),
            StructField('avg(TimeinMin)', Double(), True)
            ])
    expected_data = [("Easy", 19.625),
                    ("Medium", 40.0),
                    ("Hard", 174.481)]
    expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
    df5 = transform_data(input_df)
    #4. Assert the output of the transformation to the expected data frame.
    field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
    fields1 = [*map(field_list, df.schema.fields)]
    fields2 = [*map(field_list, expected_df.schema.fields)]
    # Compare schema of df and expected_df
    res = set(fields1) == set(fields2)
    # assert
    self.assertTrue(res)
    # Compare data in df and expected_df
    self.assertEqual(sorted(expected_df.collect()), sorted(df.collect()))
