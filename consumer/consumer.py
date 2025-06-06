import os
import json
import time
import pandas as pd
from kafka import KafkaConsumer

KAFKA_BROKER = os.getenv('KAFKA_BROKER')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
GROUP_ID = os.getenv('GROUP_ID')
BATCH_SIZE = int(os.getenv('BATCH_SIZE', 1000))
OUTPUT_DIR = os.getenv('OUTPUT_DIR', '/app/data_batches')
NUM_BATCHES_TO_WRITE = int(os.getenv('NUM_BATCHES_TO_WRITE', 3))

def create_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BROKER.split(','),
                auto_offset_reset='earliest',
                group_id=GROUP_ID,
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            print("Kafka Consumer connected successfully.")
            return consumer
        except Exception as e:
            print(f"Failed to connect to Kafka: {e}. Retrying in 5 seconds...")
            time.sleep(5)

def consume_and_write_batches(consumer):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    batch_data = []
    batch_count = 0

    print("Starting to consume messages...")
    for message in consumer:
        batch_data.append(message.value)
        
        if len(batch_data) >= BATCH_SIZE:
            output_path = os.path.join(OUTPUT_DIR, f'batch_{batch_count}.csv')
            df = pd.DataFrame(batch_data)
            df.to_csv(output_path, index=False)
            print(f"Batch {batch_count} written to {output_path} with {len(batch_data)} records.")
            
            batch_data = []
            batch_count += 1

            if batch_count >= NUM_BATCHES_TO_WRITE:
                print(f"All {NUM_BATCHES_TO_WRITE} batches have been written. Exiting.")
                break
    
    # Menulis sisa data jika ada (dan jika belum mencapai target batch)
    if batch_data and batch_count < NUM_BATCHES_TO_WRITE:
        output_path = os.path.join(OUTPUT_DIR, f'batch_{batch_count}.csv')
        df = pd.DataFrame(batch_data)
        df.to_csv(output_path, index=False)
        print(f"Final batch {batch_count} written to {output_path} with {len(batch_data)} records.")

if __name__ == "__main__":
    kafka_consumer = create_consumer()
    consume_and_write_batches(kafka_consumer)
