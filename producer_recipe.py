import time
import json
import random
import pandas as pd
from kafka import KafkaProducer
import csv # Untuk parsing CSV yang lebih robust

# Konfigurasi Kafka
KAFKA_BROKER = 'kafka:9093'
TOPIC_NAME = 'recipe-stream'
DATASET_PATH = '/app/recipes_data.csv'
COLUMNS_TO_DROP = ['source', 'link']

producer = None
while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            api_version=(0, 10, 1) # Sesuaikan jika perlu
        )
        print(f"Berhasil terhubung ke Kafka broker: {KAFKA_BROKER} untuk resep.")
    except Exception as e:
        print(f"Gagal terhubung ke Kafka broker untuk resep: {e}. Mencoba lagi dalam 10 detik...")
        time.sleep(10)

print(f"Mulai mengirim data resep dari {DATASET_PATH} ke topik: {TOPIC_NAME}")

try:
    # Menggunakan Pandas untuk membaca CSV per chunk jika file sangat besar
    # Jika tidak terlalu besar (misal < 1GB dan RAM container cukup), bisa baca sekaligus
    # Untuk file 2M resep, chunking atau baca baris demi baris lebih aman.

    # Opsi 1: Baca dengan Pandas per chunk (lebih aman untuk CSV kompleks)
    # chunk_size = 1000 # Kirim 1000 resep, lalu jeda
    # for chunk_df in pd.read_csv(DATASET_PATH, chunksize=chunk_size, keep_default_na=False, na_values=['']):
    #     chunk_df.drop(columns=COLUMNS_TO_DROP, inplace=True, errors='ignore')
    #     for index, row in chunk_df.iterrows():
    #         message = row.to_dict()
    #         producer.send(TOPIC_NAME, value=message)
    #         # print(f"Terkirim (Resep): {message.get('title', 'N/A')}") # Uncomment untuk debugging
    #     print(f"Mengirim {len(chunk_df)} resep...")
    #     producer.flush() # Pastikan terkirim sebelum sleep
    #     time.sleep(random.uniform(0.5, 2.0)) # Jeda antar chunk

    # Opsi 2: Baca baris demi baris menggunakan modul csv (lebih hemat memori)
    with open(DATASET_PATH, 'r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file) # Membaca sebagai dictionary
        line_count = 0
        for row in csv_reader:
            # Drop kolom
            for col_to_drop in COLUMNS_TO_DROP:
                if col_to_drop in row:
                    del row[col_to_drop]
            
            producer.send(TOPIC_NAME, value=row)
            line_count += 1
            if line_count % 500 == 0: # Cetak status setiap 500 baris
                 print(f"Terkirim {line_count} resep. Contoh judul: {row.get('title', 'N/A')}")
                 producer.flush() # Kirim buffer secara periodik

            time.sleep(random.uniform(0.001, 0.01)) # Jeda kecil per baris

    print("Semua data dari CSV telah dikirim.")

except FileNotFoundError:
    print(f"Error: File dataset '{DATASET_PATH}' tidak ditemukan.")
except Exception as e:
    print(f"Error pada producer resep: {e}")
finally:
    if producer:
        print("Flushing sisa pesan...")
        producer.flush()
        producer.close()
        print("Producer resep ditutup.")