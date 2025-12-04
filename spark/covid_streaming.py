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
        df.write \
            .mode("append") \
            .format("jdbc") \
            .option("url", JDBC_URL) \
            .option("dbtable", table_name) \
            .option("user", JDBC_PROPERTIES["user"]) \
            .option("password", JDBC_PROPERTIES["password"]) \
            .option("driver", JDBC_PROPERTIES["driver"]) \
            .save()
        logger.info(f" Batch {epoch_id}: {count} records written to {table_name}")
    except Exception as e:
        logger.error(f" MySQL Error for batch {epoch_id} - {table_name}: {str(e)}", exc_info=True)

def predict_future_trends(df, epoch_id):
    
    try:
        if df.rdd.isEmpty():
            logger.info(f"Batch {epoch_id}: No data for prediction")
            return None
        
        window_7days = Window.partitionBy("location").orderBy("date").rowsBetween(-6, 0)
        window_14days = Window.partitionBy("location").orderBy("date").rowsBetween(-13, 0)
        window_lag = Window.partitionBy("location").orderBy("date")
        
        predictions = df \
            .withColumn("avg_new_cases_7d", avg("new_cases").over(window_7days)) \
            .withColumn("avg_new_cases_14d", avg("new_cases").over(window_14days)) \
            .withColumn("avg_new_deaths_7d", avg("new_deaths").over(window_7days)) \
            .withColumn("total_cases_yesterday", lag("total_cases", 1).over(window_lag)) \
            .withColumn("daily_growth_rate", 
                       when(col("total_cases_yesterday") > 0, 
                            (col("total_cases") - col("total_cases_yesterday")) / col("total_cases_yesterday"))
                       .otherwise(0.0)) \
            .withColumn("avg_growth_rate_7d", avg("daily_growth_rate").over(window_7days)) \
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
        
        result = predictions.select(
            "location", "iso_code", "continent", "date",
            "total_cases", "new_cases", "total_deaths", "new_deaths",
            "avg_new_cases_7d", "avg_new_cases_14d", "avg_new_deaths_7d",
            "daily_growth_rate", "avg_growth_rate_7d",
            "predicted_new_cases_next_day", "predicted_total_cases_next_day", 
            "predicted_new_deaths_next_day",
            "trend_direction", "prediction_confidence",
            "prediction_date", "predicted_at"
        ).filter(col("predicted_new_cases_next_day").isNotNull())
        
        if not result.rdd.isEmpty():
            logger.info(f"Batch {epoch_id}: Generated {result.count()} predictions")
            return result
        else:
            return None
            
    except Exception as e:
        logger.error(f"✗ Prediction error in batch {epoch_id}: {str(e)}", exc_info=True)
        return None

def process_batch(df, epoch_id):
    try:
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing Batch {epoch_id}")
        logger.info(f"{'='*60}")
        
        if df.rdd.isEmpty():
            logger.info(f"Batch {epoch_id} is empty, skipping")
            return

        processed = df \
            .withColumn("new_cases", coalesce(col("new_cases").cast(DoubleType()), lit(0.0))) \
            .withColumn("total_cases", coalesce(col("total_cases").cast(DoubleType()), lit(0.0))) \
            .withColumn("total_deaths", coalesce(col("total_deaths").cast(DoubleType()), lit(0.0))) \
            .withColumn("new_deaths", coalesce(col("new_deaths").cast(DoubleType()), lit(0.0))) \
            .withColumn("active_cases", coalesce(col("active_cases").cast(DoubleType()), lit(0.0))) \
            .withColumn("population", coalesce(col("population").cast(DoubleType()), lit(0.0))) \
            .withColumn("recovery_rate", coalesce(col("recovery_rate").cast(DoubleType()), lit(0.0))) \
            .withColumn("death_rate", coalesce(col("death_rate").cast(DoubleType()), lit(0.0))) \
            .withColumn("cases_per_million", coalesce(col("cases_per_million").cast(DoubleType()), lit(0.0))) \
            .withColumn("deaths_per_million", coalesce(col("deaths_per_million").cast(DoubleType()), lit(0.0))) \
            .withColumn("new_cases_ratio", coalesce(col("new_cases_ratio").cast(DoubleType()), lit(0.0))) \
            .withColumn("cases_to_population_ratio", coalesce(col("cases_to_population_ratio").cast(DoubleType()), lit(0.0))) \
            .withColumn("date", to_date(col("date"))) \
            .withColumn("timestamp", to_timestamp(col("timestamp"))) \
            .withColumn("processing_time", current_timestamp()) \
            .withColumn("fatality_rate", 
                       when(col("total_cases") > 0, col("total_deaths") / col("total_cases"))
                       .otherwise(0.0)) \
            .withColumn("active_cases_ratio", 
                       when(col("total_cases") > 0, col("active_cases") / col("total_cases"))
                       .otherwise(0.0)) \
            .withColumn("recovery_rate_calculated", 
                       when(col("total_cases") > 0, 
                            (col("total_cases") - col("active_cases") - col("total_deaths")) / col("total_cases"))
                       .otherwise(0.0)) \
            .withColumn("growth_rate", 
                       when(col("total_cases") > 0, col("new_cases") / col("total_cases"))
                       .otherwise(0.0)) \
            .withColumn("severity_level", 
                       when(col("death_rate") > 0.05, lit("High"))
                       .when(col("death_rate") > 0.02, lit("Medium"))
                       .otherwise(lit("Low")))

        record_count = processed.count()
        logger.info(f" Processed {record_count} records")

        write_to_mysql(processed, epoch_id, "covid_realtime_stats")

        predictions = predict_future_trends(processed, epoch_id)
        if predictions is not None:
            write_to_mysql(predictions, epoch_id, "covid_predictions")
            
            top_predictions = predictions.orderBy(desc("predicted_new_cases_next_day")).limit(5)
            logger.info("\n Top 5 Countries - Predicted New Cases Tomorrow:")
            for row in top_predictions.collect():
                logger.info(f"   • {row.location}: {row.predicted_new_cases_next_day:.0f} cases "
                          f"(Trend: {row.trend_direction}, Confidence: {row.prediction_confidence})")

        windowed_stats = processed \
            .withWatermark("timestamp", "10 minutes") \
            .groupBy(
                window(col("timestamp"), "5 minutes"),
                "location", "iso_code"
            ) \
            .agg(
                sum("new_cases").alias("total_new_cases_window"),
                sum("new_deaths").alias("total_new_deaths_window"),
                avg("death_rate").alias("avg_death_rate_window"),
                max("total_cases").alias("max_total_cases"),
                last("active_cases").alias("latest_active_cases")
            ) \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                "location", "iso_code",
                "total_new_cases_window",
                "total_new_deaths_window",
                "avg_death_rate_window",
                "max_total_cases",
                "latest_active_cases",
                current_timestamp().alias("processed_at")
            )
        
        if not windowed_stats.rdd.isEmpty():
            write_to_mysql(windowed_stats, epoch_id, "windowed_covid_stats")

        continent_stats = processed \
            .withWatermark("timestamp", "10 minutes") \
            .groupBy(
                window(col("timestamp"), "5 minutes"),
                "continent"
            ) \
            .agg(
                sum("new_cases").alias("continent_new_cases"),
                sum("new_deaths").alias("continent_new_deaths"),
                avg("death_rate").alias("continent_avg_death_rate"),
                countDistinct("location").alias("countries_count"),
                sum("total_cases").alias("continent_total_cases")
            ) \
            .select(
                col("window.start").alias("continent_window_start"),
                col("window.end").alias("continent_window_end"),
                "continent",
                "continent_new_cases",
                "continent_new_deaths",
                "continent_avg_death_rate",
                "countries_count",
                "continent_total_cases",
                current_timestamp().alias("processed_at")
            )
        
        if not continent_stats.rdd.isEmpty():
            write_to_mysql(continent_stats, epoch_id, "continent_covid_stats")

        hotspots = processed \
            .filter((col("is_hotspot") == "true") | (col("new_cases") > 1000) | (col("death_rate") > 0.05)) \
            .select(
                "location", "iso_code", "total_cases", "new_cases", 
                "death_rate", "active_cases", "timestamp",
                current_timestamp().alias("detected_at")
            )
        
        if not hotspots.rdd.isEmpty():
            write_to_mysql(hotspots, epoch_id, "covid_hotspots")
            logger.info(f" {hotspots.count()} hotspots detected")

        country_rankings = processed \
            .groupBy("location", "iso_code") \
            .agg(
                max("total_cases").alias("max_cases_country"),
                sum("new_cases").alias("total_new_cases_country"),
                avg("death_rate").alias("avg_death_rate_country")
            ) \
            .orderBy(desc("max_cases_country")) \
            .withColumn("ranking_position", row_number().over(Window.orderBy(desc("max_cases_country")))) \
            .withColumn("updated_at", current_timestamp()) \
            .limit(50)
        
        if not country_rankings.rdd.isEmpty():
            write_to_mysql(country_rankings, epoch_id, "country_rankings")

        logger.info(f"✓ Batch {epoch_id} completed successfully\n")

    except Exception as e:
        logger.error(f"✗ Error in batch {epoch_id}: {str(e)}", exc_info=True)

def main():
    global spark
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info("\n" + "="*60)
    logger.info("Starting COVID-19 Real-Time Analytics System")
    logger.info("    Real-time monitoring")
    logger.info("    Hotspot identification") 
    logger.info("    Future trends prediction")
    logger.info("="*60 + "\n")

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "covid19_processed") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    logger.info(" Connected to Kafka topic: covid19_processed")

    parsed = kafka_df \
        .selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), RAW_SCHEMA).alias("data")) \
        .select("data.*")

    query = parsed.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/checkpoints/covid_mysql") \
        .trigger(processingTime="30 seconds") \
        .start()

    logger.info(" Streaming query started successfully")
    logger.info("  Processing interval: 30 seconds")
    logger.info(" Checkpoint location: /tmp/checkpoints/covid_mysql\n")

    query.awaitTermination()

if __name__ == "__main__":
    main()
