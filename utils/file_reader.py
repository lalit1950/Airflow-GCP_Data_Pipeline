from config.spark_config import get_spark_session

def Read_Files(pipeline_name : str,file_type:str, file_path: str):
    spark = get_spark_session(pipeline_name)
    df = (
        spark.read
        .format(file_type)
        .option("inferSchema", True)
        .option("header", True)
        .load(file_path)
    )
    return df