from kafka import KafkaProducer
import csv
import json
import time
import os

def main():
    # Connect to Kafka
    producer = KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    topic = 'covid19_raw'
    
    # Send data
    csv_file = 'owid-covid-data.csv'
    
    if not os.path.exists(csv_file):
        print(f"Error: {csv_file} not found!")
        print("Current directory:", os.getcwd())
        print("Files:", os.listdir())
        return
    
    try:
        with open(csv_file, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            
            for row in reader:
                # Send raw data as-is (NiFi will handle transformations)
                producer.send(topic, row)
                print(f"Sent: {row.get('location', 'Unknown')} - {row.get('date', '')}")
                time.sleep(0.1)  # Simulate real-time
                
    except Exception as e:
        print(f"Error: {e}")
    
    producer.flush()
    producer.close()
    print("Producer finished")

if __name__ == "__main__":
    main()