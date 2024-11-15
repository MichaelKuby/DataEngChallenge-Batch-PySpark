from pyspark.sql import SparkSession

spark_session = None


def get_spark_session(app_name="DataEngChallenge-Batch-PySpark"):
    global spark_session
    if spark_session is None:
        spark_session = (
            SparkSession.builder.appName(app_name)
            .config("spark.log.level", "ERROR")
            .config("spark.driver.memory", "4g")
            .master("local[*]")
            .getOrCreate()
        )
    return spark_session
