#!/bin/bash
# في ملف ./spark/entrypoint.sh

# انتظر شوية عشان كل الخدمات تبتدى
sleep 30

# ابدأ الـ Spark application
echo "Starting Spark Streaming Application..."

# استخدم المسار الكامل لـ spark-submit
/opt/spark/bin/spark-submit \
    --master ${SPARK_MASTER:-spark://spark-master:7077} \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,com.microsoft.sqlserver:mssql-jdbc:11.2.1.jre11 \
    --conf "spark.sql.streaming.checkpointLocation=/tmp/checkpoints" \
    covid_streaming.py