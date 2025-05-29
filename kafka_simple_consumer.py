import time
import json
from kafka import KafkaConsumer
import os

# Konfigurasi Kafka
KAFKA_BROKER = 'kafka:9093'
TOPIC_NAME = 'recipe-stream'
GROUP_ID = 'recipe-raw-batcher-group'
BATCH_SIZE = 10000  # Jumlah pesan per file batch (sesuaikan, misal 10k atau 50k)
OUTPUT_DIR = '/data_staging/raw_recipe_batches/'

os.makedirs(OUTPUT_DIR, exist_ok=True)

consumer = None
while consumer is None:
    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset='earliest',
            group_id=GROUP_ID,
            consumer_timeout_ms=10000 # Timeout agar tidak hang jika tidak ada pesan baru
        )
        print(f"Consumer terhubung ke Kafka: {KAFKA_BROKER}, topik: {TOPIC_NAME}")
    except Exception as e:
        print(f"Gagal terhubung ke Kafka sebagai consumer: {e}. Mencoba lagi dalam 10 detik...")
        time.sleep(10)

current_batch = []
batch_counter = 1
processed_count_total = 0

try:
    while True: # Loop untuk terus mencoba membaca pesan
        messages_in_poll = 0
        for message in consumer: # Ini akan blok sampai pesan datang atau timeout
            messages_in_poll +=1
            try:
                data_str = message.value.decode('utf-8')
                # Validasi JSON (opsional tapi baik)
                # json.loads(data_str) 
                current_batch.append(data_str)
                processed_count_total += 1

                if processed_count_total % 1000 == 0:
                    print(f"Total resep diproses oleh simple consumer: {processed_count_total}")

            except Exception as e:
                print(f"Gagal decode atau parse pesan: {message.value}, error: {e}")
                continue

            if len(current_batch) >= BATCH_SIZE:
                file_path = os.path.join(OUTPUT_DIR, f'raw_batch_{batch_counter:04d}.jsonl')
                with open(file_path, 'w', encoding='utf-8') as f:
                    for record_str in current_batch:
                        f.write(record_str + '\n')
                print(f"Batch {batch_counter} disimpan ke {file_path} dengan {len(current_batch)} resep.")
                current_batch = []
                batch_counter += 1
        
        if messages_in_poll == 0:
            print("Tidak ada pesan baru dalam interval timeout, menunggu lagi...")
            # Jika tidak ada pesan, loop akan berlanjut setelah timeout dari consumer_timeout_ms
            # Jika producer sudah selesai dan semua pesan sudah dikonsumsi,
            # mungkin perlu logika untuk keluar dari loop ini atau simpan batch terakhir.

except KeyboardInterrupt:
    print("Simple consumer dihentikan oleh pengguna.")
except Exception as e:
    print(f"Error pada simple consumer: {e}")
finally:
    if current_batch:
        file_path = os.path.join(OUTPUT_DIR, f'raw_batch_{batch_counter:04d}_final.jsonl')
        with open(file_path, 'w', encoding='utf-8') as f:
            for record_str in current_batch:
                f.write(record_str + '\n')
        print(f"Batch final {batch_counter} disimpan ke {file_path} dengan {len(current_batch)} resep.")
    if consumer:
        consumer.close()
        print("Simple consumer ditutup.")