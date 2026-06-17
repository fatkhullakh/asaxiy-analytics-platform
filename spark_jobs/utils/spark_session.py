"""
Shared Spark session factory for all Silver jobs
Configured for Delta Lake + MinIO (S3)
"""
from pyspark.sql import SparkSession


def get_spark_session(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("spark://192.168.0.104:7077")
        .config("spark.jars.packages",
                "io.delta:delta-spark_2.12:3.2.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # MinIO S3 config
        .config("spark.hadoop.fs.s3a.endpoint",        "http://192.168.0.104:9000")
        .config("spark.hadoop.fs.s3a.access.key",      "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key",      "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark