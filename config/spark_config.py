import os
import findspark
from pyspark.sql import SparkSession
os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-11"
findspark.init()  # This links your Python environment to local Spark

def get_spark_session(app_name: str = "MedallionPipeline"):
    spark= (SparkSession
        .builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    return spark

