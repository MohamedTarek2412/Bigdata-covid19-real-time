FROM apache/spark:3.5.3

ARG SPARK_PACKAGES

USER root

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    unixodbc-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    pyodbc==4.0.39 \
    kafka-python==2.0.2 \
    pandas \
    pyarrow==14.0.2

COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# 3. حمل MySQL driver مباشرة
RUN curl -L -o /opt/spark/jars/mysql-connector-java-8.0.33.jar \
    https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.33/mysql-connector-java-8.0.33.jar

# 4. انسخ الكود
COPY . /app
WORKDIR /app

# 5. أنشئ checkpoints directory
RUN mkdir -p /tmp/checkpoints

# 6. إعداد environment
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYSPARK_PYTHON=python3

# 7. CMD صحيح
CMD ["spark-submit", \
    "--master", "spark://spark-master:7077", \
    "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,mysql:mysql-connector-java:8.0.33", \
    "--conf", "spark.driver.memory=512m", \
    "--conf", "spark.executor.memory=512m", \
    "--conf", "spark.sql.shuffle.partitions=2", \
    "--conf", "spark.sql.streaming.checkpointLocation=/tmp/checkpoints", \
    "/app/covid_streaming.py"]