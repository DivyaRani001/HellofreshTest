from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
from pyspark.sql.functions import when, col

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,BooleanType,DoubleType
import configparser
#Read the configs
config = configparser.ConfigParser()
config.read(r'/root/config.ini')
inputLocation = config.get('path', 'inputLocation')
outputLocation = config.get('path','outputLocation')


spark = SparkSession.builder \
    .master("local[1]") \
    .appName("hellofresh.com") \
    .getOrCreate()

# Read JSON file into dataframe
df = spark.read.json(inputLocation)
#df.printSchema()
#df.show()

##TASK2:
##1) Extract only recipes that have beef as one of the ingredients.

beefDF=df.filter(df.ingredients.contains('Beef')|df.ingredients.contains('beef'))
#beefDF.show()
#beefDF.count()

##2) Calculate average cooking time duration per difficulty level.

dfTimeConversionCookTime = beefDF.withColumn(
    'cookTime',
    F.coalesce(F.regexp_extract('cookTime', r'(\d+)H', 1).cast('int'), F.lit(0)) * 3600 +
    F.coalesce(F.regexp_extract('cookTime', r'(\d+)M', 1).cast('int'), F.lit(0)) * 60 +
    F.coalesce(F.regexp_extract('cookTime', r'(\d+)S', 1).cast('int'), F.lit(0))
        )
#dfTimeConversionCookTime.show()
#dfTimeConversionCookTime.count()

dfTimeConversionPrepTime = dfTimeConversionCookTime.withColumn(
    'prepTime',
    F.coalesce(F.regexp_extract('prepTime', r'(\d+)H', 1).cast('int'), F.lit(0)) * 3600 +
    F.coalesce(F.regexp_extract('prepTime', r'(\d+)M', 1).cast('int'), F.lit(0)) * 60 +
    F.coalesce(F.regexp_extract('prepTime', r'(\d+)S', 1).cast('int'), F.lit(0))
        )
#dfTimeConversionPrepTime.show()
#dfTimeConversionPrepTime.count() ##need to change the name

dfTotalCookTimeSeconds = dfTimeConversionPrepTime.withColumn("TotalCookTime", col("cookTime")+col("prepTime"))

#UserDefinedFunction(udf) for convreting seconds to minutes

def convert(seconds):
    minutes = seconds // 60
    seconds %= 60
    return "%02d" % (minutes)

#import below (can be imported initially)
#from pyspark.sql.functions import udf
#from pyspark.sql.types import StringType
#from pyspark.sql import functions as F
#from pyspark.sql.functions import when, col

apply_my_udf = udf(lambda z: convert(z), StringType())
dfTimeInMins = dfTotalCookTimeSeconds.withColumn("TimeinMin", apply_my_udf(dfTotalCookTimeSeconds.TotalCookTime))
dfTimeInMins.show()

dfDifficulty=dfTimeInMins.withColumn(
    "Difficulty",
    F.when(F.col("TimeinMin")>= 60, 'Hard').otherwise(
        F.when(F.col("TimeinMin") < 30, 'Easy').otherwise('Medium')
    ),
)
#dfDifficulty.show()

#Average calculation time for cooking

dfAvgCookTime=dfDifficulty.groupBy("Difficulty").agg({"TimeinMin":"avg"},)
#dfAvgCookTime.show()

dfAvgCookTime.write.csv(outputLocation,header='true')
