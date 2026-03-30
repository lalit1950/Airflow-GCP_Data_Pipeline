import pyspark.sql.functions as F
from pyspark.sql import SparkSession

def get_spark_session(app_name: str = "MedallionPipeline"):
    spark= (SparkSession
        .builder
        .appName(app_name)
        .master("local[*]")
        .getOrCreate()
    )
    return spark

spark = get_spark_session("SilverLayer")

print("Reading CSV from bronze layer...")

df = spark.read.parquet("gs://lk_1995/bronze/BigFake/")

print("Data preview:")
df.show(5)

datacount=df.count()

print(f"Total data count is : {datacount}")

print("data cleansing in silver layer...")

clean_df = df \
    .dropDuplicates() \
    .withColumn("date", F.to_date(F.col("Date")))

print("Writing Silver layer to GCS...")

clean_df.write \
    .mode("overwrite") \
    .parquet("gs://lk_1995/silver/BigFake/")

print("Silver layer completed")