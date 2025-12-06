import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("covid_stream_analytics")

def get_spark(app_name="COVID-Analytics"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,mysql:mysql-connector-java:8.0.33") \
        .config("spark.driver.memory", "512m") \
        .config("spark.executor.memory", "1g") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

JDBC_URL = "jdbc:mysql://mysql:3306/covid_db?useSSL=false"
JDBC_PROPERTIES = {
    "user": "sa",
    "password": "P@ssw0rd123",
    "driver": "com.mysql.cj.jdbc.Driver"
}

RAW_SCHEMA = StructType([
    StructField("uuid", StringType(), True),
    StructField("continent", StringType(), True),
    StructField("location", StringType(), True),
    StructField("iso_code", StringType(), True),
    StructField("date", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("total_cases", StringType(), True),
    StructField("new_cases", StringType(), True),
    StructField("total_deaths", StringType(), True),
    StructField("new_deaths", StringType(), True),
    StructField("active_cases", StringType(), True),
    StructField("population", StringType(), True),
    StructField("recovery_rate", StringType(), True),
    StructField("death_rate", StringType(), True),
    StructField("cases_per_million", StringType(), True),
    StructField("deaths_per_million", StringType(), True),
    StructField("new_cases_ratio", StringType(), True),
    StructField("cases_to_population_ratio", StringType(), True),
    StructField("is_hotspot", StringType(), True)
])

def write_to_mysql(df, epoch_id, table_name):
    try:
        if df.rdd.isEmpty():
            logger.info(f"Batch {epoch_id}: Empty DataFrame for {table_name}, skipping")
            return
        count = df.count()
        df.write.mode("append").format("jdbc") \
            .option("url", JDBC_URL)            .option("dbtable", table_name)            .option("user", JDBC_PROPERTIES["user"])            .option("password", JDBC_PROPERTIES["password"])            .option("driver", JDBC_PROPERTIES["driver"])            .save()
        logger.info(f"Batch {epoch_id}: {count} records written to {table_name}")
    except Exception as e:
        logger.error(f"MySQL Error for batch {epoch_id} - {table_name}: {str(e)}", exc_info=True)

def predict_future_trends(df, epoch_id):
    try:
        if df.rdd.isEmpty():
            return None
        w7 = Window.partitionBy("location").orderBy("date").rowsBetween(-6, 0)
        w14 = Window.partitionBy("location").orderBy("date").rowsBetween(-13, 0)
        wlag = Window.partitionBy("location").orderBy("date")

        pred = df \
            .withColumn("avg_new_cases_7d", avg("new_cases").over(w7)) \
            .withColumn("avg_new_cases_14d", avg("new_cases").over(w14)) \
            .withColumn("avg_new_deaths_7d", avg("new_deaths").over(w7)) \
            .withColumn("total_cases_yesterday", lag("total_cases", 1).over(wlag)) \
            .withColumn("daily_growth_rate", when(col("total_cases_yesterday") > 0,
                            (col("total_cases") - col("total_cases_yesterday")) / col("total_cases_yesterday"))
                            .otherwise(0.0)) \
            .withColumn("avg_growth_rate_7d", avg("daily_growth_rate").over(w7)) \
            .withColumn("predicted_new_cases_next_day",
                            col("avg_new_cases_7d") * (1 + col("avg_growth_rate_7d"))) \
            .withColumn("predicted_total_cases_next_day",
                            col("total_cases") + col("predicted_new_cases_next_day")) \
            .withColumn("predicted_new_deaths_next_day",
                            col("avg_new_deaths_7d") * (1 + col("avg_growth_rate_7d"))) \
            .withColumn("trend_direction",
                            when(col("avg_new_cases_7d") > col("avg_new_cases_14d"), lit("Increasing"))
                            .when(col("avg_new_cases_7d") < col("avg_new_cases_14d"), lit("Decreasing"))
                            .otherwise(lit("Stable"))) \
            .withColumn("prediction_confidence",
                            when(abs(col("avg_growth_rate_7d")) < 0.05, lit("High"))
                            .when(abs(col("avg_growth_rate_7d")) < 0.15, lit("Medium"))
                            .otherwise(lit("Low"))) \
            .withColumn("predicted_at", current_timestamp()) \
            .withColumn("prediction_date", date_add(col("date"), 1))

        result = pred.select(
            "location", "iso_code", "continent", "date", "total_cases", "new_cases",
            "total_deaths", "new_deaths", "avg_new_cases_7d", "avg_new_cases_14d",
            "avg_new_deaths_7d", "daily_growth_rate", "avg_growth_rate_7d",
            "predicted_new_cases_next_day", "predicted_total_cases_next_day",
            "predicted_new_deaths_next_day", "trend_direction", "prediction_confidence",
            "prediction_date", "predicted_at"
        ).filter(col("predicted_new_cases_next_day").isNotNull())

        return result if not result.rdd.isEmpty() else None
    except Exception as e:
        logger.error(f"Prediction error: {e}", exc_info=True)
        return None

def process_batch(df, epoch_id):
    try:
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing Batch {epoch_id}")
        logger.info(f"{'='*60}")
        if df.rdd.isEmpty():
            logger.info("Batch is empty")
            return

        cleaned = (df
            .withColumn("total_cases",   when(trim(col("total_cases")).isin("", "null", "NULL"), lit(0.0))
                                        .otherwise(trim(col("total_cases")).cast("double")))
            .withColumn("new_cases",     when(trim(col("new_cases")).isin("", "null", "NULL"), lit(0.0))
                                        .otherwise(trim(col("new_cases")).cast("double")))
            .withColumn("total_deaths",  when(trim(col("total_deaths")).isin("", "null", "NULL"), lit(0.0))
                                        .otherwise(trim(col("total_deaths")).cast("double")))
            .withColumn("new_deaths",    when(trim(col("new_deaths")).isin("", "null", "NULL"), lit(0.0))
                                        .otherwise(trim(col("new_deaths")).cast("double")))
            .withColumn("active_cases",  when(trim(col("active_cases")).isin("", "null", "NULL"), lit(0.0))
                                        .otherwise(trim(col("active_cases")).cast("double")))
            .withColumn("population",    when(trim(col("population")).isin("", "null", "NULL"), lit(0.0))
                                        .otherwise(trim(col("population")).cast("double")))
            .withColumn("death_rate",    when(col("total_cases") > 0,
                                        round(col("total_deaths") / col("total_cases"), 6)).otherwise(0.0))
            .withColumn("cases_per_million", when(col("population") > 0,
                                        round(col("total_cases") / col("population") * 1000000, 2)).otherwise(0.0))
            .withColumn("deaths_per_million", when(col("population") > 0,
                                        round(col("total_deaths") / col("population") * 1000000, 2)).otherwise(0.0))
            .withColumn("new_cases_ratio", when(col("total_cases") > 0,
                                        round(col("new_cases") / col("total_cases"), 6)).otherwise(0.0))
            .withColumn("cases_to_population_ratio", when(col("population") > 0,
                                        round(col("total_cases") / col("population"), 6)).otherwise(0.0))
            .withColumn("recovery_rate", when(col("total_cases") > 0,
                                        round((col("total_cases") - col("active_cases") - col("total_deaths")) / col("total_cases"), 6))
                                        .otherwise(0.0))
            .withColumn("date", to_date(col("date")))
            .withColumn("timestamp", to_timestamp(col("timestamp")))
            .withColumn("processing_time", current_timestamp())
            .withColumn("is_hotspot", col("is_hotspot").cast("boolean"))
        )

        
        realtime = cleaned.dropDuplicates(["location", "date"])
        write_to_mysql(realtime, epoch_id, "covid_realtime_stats")

        
        preds = predict_future_trends(realtime, epoch_id)
        if preds:
            write_to_mysql(preds.dropDuplicates(["location", "prediction_date"]), epoch_id, "covid_predictions")

        
        continent = (cleaned
            .withWatermark("timestamp", "10 minutes")
            .groupBy(window(col("timestamp"), "5 minutes"), "continent")
            .agg(
                sum("new_cases").alias("continent_new_cases"),
                sum("new_deaths").alias("continent_new_deaths"),
                avg("death_rate").alias("continent_avg_death_rate"),
                countDistinct("location").alias("countries_count"),
                sum("total_cases").alias("continent_total_cases")
            )
            .select(
                col("window.start").alias("continent_window_start"),
                col("window.end").alias("continent_window_end"),
                "continent", "continent_new_cases", "continent_new_deaths",
                "continent_avg_death_rate", "countries_count", "continent_total_cases",
                current_timestamp().alias("processed_at")
            )
            .dropDuplicates(["continent_window_start", "continent"])
        )
        if not continent.rdd.isEmpty():
            write_to_mysql(continent, epoch_id, "continent_covid_stats")

      
        hotspots = (cleaned
            .filter((col("new_cases") > 10000) | (col("death_rate") > 0.05) | (col("is_hotspot") == True))
            .withColumn("detected_date", to_date("timestamp"))
            .dropDuplicates(["location", "detected_date"])
            .select("location","iso_code","total_cases","new_cases","death_rate",
                    "active_cases","timestamp", current_timestamp().alias("detected_at"))
        )
        if not hotspots.rdd.isEmpty():
            write_to_mysql(hotspots, epoch_id, "covid_hotspots")

        logger.info(f"Batch {epoch_id} finished perfectly!\n")

    except Exception as e:
        logger.error(f"Batch {epoch_id} failed: {e}", exc_info=True)

def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("COVID-19 Streaming Analytics STARTED – CLEAN & FAST")

    df = (spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "kafka:9092")
          .option("subscribe", "covid19_processed")
          .option("startingOffsets", "earliest")
          .option("failOnDataLoss", "false")
          .load())

    parsed = (df
              .selectExpr("CAST(value AS STRING) as json")
              .select(from_json(col("json"), RAW_SCHEMA).alias("data"))
              .select("data.*"))

    query = (parsed.writeStream
             .foreachBatch(process_batch)
             .outputMode("append")
             .option("checkpointLocation", "/tmp/checkpoints/covid_mysql")
             .trigger(processingTime="30 seconds")
             .start())

    query.awaitTermination()

if __name__ == "__main__":
    main()
