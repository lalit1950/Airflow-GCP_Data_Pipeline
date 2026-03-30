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

spark = get_spark_session("GoldLayer")

print("Reading CSV from silver layer...")

df = spark.read.parquet("gs://lk_1995/silver/BigFake/")

print("Data preview:")
df.show(5)
datacount=df.count()
print(f"Total data count is : {datacount}")
print("data aggrigating in gold layer...")

gold_df = df.groupBy("Company").agg(
    F.count("*").alias("total_Count")
)

print("Writing Gold layer to GCS...")

gold_df.write \
    .mode("overwrite") \
    .parquet("gs://lk_1995/gold/BigFake/")

print("Writing bigquery..")

gold_df.write \
  .format("bigquery") \
  .option("table","project-6fd5b69f-0988-4afc-9c6.Dataset.sales_gold") \
  .option("parentProject","project-6fd5b69f-0988-4afc-9c6") \
  .option("temporaryGcsBucket","lk_1995") \
  .mode("overwrite") \
  .save()



print("Gold layer completed")