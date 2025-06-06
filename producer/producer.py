import os
import json
import time
import random
import pandas as pd
import ast 
from kafka import KafkaProducer

KAFKA_BROKER = os.getenv('KAFKA_BROKER')
DATASET_PATH = os.getenv('DATASET_PATH')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
MAX_ROWS_TO_SEND = int(os.getenv('MAX_ROWS_TO_SEND', 30005))

def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER.split(','),
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Kafka Producer connected successfully.")
            return producer
        except Exception as e:
            print(f"Failed to connect to Kafka: {e}. Retrying in 5 seconds...")
            time.sleep(5)

def stream_data(producer, topic):
    try:
        print(f"Reading dataset from {DATASET_PATH}")
        df = pd.read_csv(DATASET_PATH, nrows=MAX_ROWS_TO_SEND)
        
        # Fokus pada kolom yang relevan dari file sumber
        df_relevant = df[['title', 'NER']].dropna()

        print(f"Streaming {len(df_relevant)} rows to topic '{topic}'...")

        for index, row in df_relevant.iterrows():
            try:
                ingredients_list = ast.literal_eval(row['NER'])
            except (ValueError, SyntaxError):
                continue
            
            message = {
                "Title": row['title'],
                "Cleaned_Ingredients": ingredients_list 
            }
            
            producer.send(topic, value=message)
            
            if (index + 1) % 1000 == 0:
                print(f"Sent: {index + 1} messages...")

        producer.flush()
        print("Finished streaming data.")

    except FileNotFoundError:
        print(f"Error: Dataset file not found at {DATASET_PATH}.")
    except Exception as e:
        print(f"An error occurred during streaming: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    kafka_producer = create_producer()
    stream_data(kafka_producer, KAFKA_TOPIC)
