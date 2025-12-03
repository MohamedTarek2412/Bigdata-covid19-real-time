FROM python:3.9-slim

WORKDIR /app

# تحديث حزم النظام
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

# تثبيت المكتبات المطلوبة
RUN pip install --no-cache-dir kafka-python

# نسخ ملفات البرنامج
COPY covid_producer.py .
COPY owid-covid-data.csv .

# تشغيل البرنامج
CMD ["python", "covid_producer.py"]