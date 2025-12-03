import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("covid_stream")

def get_spark(app_name="COVID-Stream"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,mysql:mysql-connector-java:8.0.33") \
        .config("spark.driver.memory", "512m") \
        .config("spark.executor.memory", "512m") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

JDBC_URL = "jdbc:mysql://mysql:3306/covid_db?useSSL=false"
JDBC_PROPERTIES = {
    "user": "sa",
    "password": "P@ssw0rd123",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# أبسط schema
RAW_SCHEMA = StructType([
    StructField("location", StringType(), True),
    StructField("new_cases", StringType(), True),
    StructField("new_deaths", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("total_cases", StringType(), True),
    StructField("is_hotspot", StringType(), True)
])

def write_to_mysql(df, epoch_id):
    try:
        if df.isEmpty():
            return
        
        # اكتب إلى MySQL
        df.write \
            .mode("append") \
            .format("jdbc") \
            .option("url", JDBC_URL) \
            .option("dbtable", "covid_realtime_stats") \
            .option("user", JDBC_PROPERTIES["user"]) \
            .option("password", JDBC_PROPERTIES["password"]) \
            .option("driver", JDBC_PROPERTIES["driver"]) \
            .save()
        
        logger.info(f"Batch {epoch_id} written to MySQL")
    except Exception as e:
        logger.error(f"Error writing to MySQL: {str(e)}")

def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Starting COVID Stream Processing...")

    # اقرأ من Kafka
    kafka_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "covid19_processed") \
        .option("startingOffsets", "latest") \
        .load()

    # حول JSON
    parsed = kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), RAW_SCHEMA).alias("d")).select("d.*")

    # معالجة بسيطة
    processed = parsed \
        .withColumn("new_cases", col("new_cases").cast("double")) \
        .withColumn("new_deaths", col("new_deaths").cast("double")) \
        .withColumn("total_cases", col("total_cases").cast("double")) \
        .withColumn("timestamp", to_timestamp(col("timestamp"))) \
        .withColumn("is_hotspot", col("is_hotspot").cast("boolean")) \
        .withColumn("processing_time", current_timestamp())

    # Stream واحد فقط - اكتب إلى Console أولاً للتأكد
    console_query = processed.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    # Stream ثاني - اكتب إلى MySQL
    mysql_query = processed.writeStream \
        .foreachBatch(write_to_mysql) \
        .outputMode("append") \
        .trigger(processingTime="60 seconds") \
        .option("checkpointLocation", "/tmp/checkpoints/mysql") \
        .start()

    logger.info("Streaming started...")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()