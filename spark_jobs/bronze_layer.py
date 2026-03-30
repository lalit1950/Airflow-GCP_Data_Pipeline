#import os
#.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-11"

from pyspark.sql import SparkSession

def get_spark_session(app_name="MedallionPipeline"):
    spark = (
        SparkSession.builder
        .appName("BronzeLayer")
        .master("local[*]")
        .getOrCreate()
    )
    return spark


spark = get_spark_session("BronzeLayer")

print("Reading CSV from local data folder...")

df = (
    spark.read
    .format("csv")
    .option("inferSchema", True)
    .option("header", True)
    .load("/opt/airflow/data/big_fake_data.csv")
)

print("Data preview:")
df.show(5)

datacount=df.count()

print(f"Total data count is : {datacount}")

print("Writing Bronze layer to GCS...")

df.write \
    .mode("overwrite") \
    .parquet("gs://lk_1995/bronze/BigFake/")

print("Bronze layer completed")