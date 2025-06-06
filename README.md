# Project 2 Big Data

Sistem ini dibuat untuk mensimulasikan alur kerja Big Data, mulai dari data streaming, pemrosesan data secara bertahap, pelatihan model machine learning, hingga penyajian model melalui API.

### Dikerjakan Oleh:
* `Muhamad Arrayyan - 5027231014`
* `RM. Novian Malcolm Bayuputra - 5027231035`
* `Dzaky Faiq Fayyadhi - 5027231047`

### Komponen
* **Apache Kafka**: Untuk data streaming dari file dataset.
* **Apache Spark**: Untuk pemrosesan data dan pelatihan model ML secara terdistribusi.
* **Flask & Gunicorn**: Untuk membangun dan menyajikan API model machine learning.
* **Docker & Docker Compose**: Untuk kontainerisasi dan pengaturan seluruh layanan.

---

## Alur Sistem

Dataset → Kafka Producer → Kafka Server → Kafka Consumer → Batch Files → Spark Script → ML Models → API → User

1.  **Dataset → Pembacaan Sekuensial oleh Kafka Producer**
    Proses dimulai dengan `Kafka Producer` yang membaca dataset resep (`recipes_data.csv`) baris demi baris.

2.  **Kafka Producer → Mengalirkan Data dengan Jeda Acak**
    `Kafka Producer` mengirimkan data resep ke Kafka Server satu per satu, dengan jeda waktu acak untuk mensimulasikan aliran data (streaming) yang realistis.

3.  **Kafka Server → Menyimpan Data di Topik `recipes`**
    `Kafka Server` bertindak sebagai perantara, menerima data dari `producer` dan menyimpannya secara berkelanjutan di dalam sebuah topik bernama `recipes`.

4.  **Kafka Consumer → Membaca dan Membuat Batch CSV**
    `Kafka Consumer` membaca data dari topik `recipes` dan mengelompokkannya ke dalam batch-batch.

5.  **File Batch → Disimpan sebagai File CSV**
    Setiap batch data yang telah dibuat oleh `consumer` disimpan sebagai file `batch_N.csv` (misalnya, `batch_0.csv`, `batch_1.csv`) di dalam volume yang dapat diakses oleh Spark.

6.  **Spark Script → Melatih 3 Set Model Secara Bertahap**
    `Spark Script` akan melatih **tiga set model** *machine learning* yang berbeda secara bertahap (inkremental):
    -   **Model 1**: Dilatih menggunakan data `batch_0.csv` (1/3 data).
    -   **Model 2**: Dilatih menggunakan data `batch_0.csv` + `batch_1.csv` (2/3 data).
    -   **Model 3**: Dilatih menggunakan semua batch, `batch_0.csv` + `batch_1.csv` + `batch_2.csv` (semua data).

7.  **Model ML → Dihasilkan per Segmen Data**
    Setiap set model terdiri dari beberapa model untuk tugas yang berbeda (`CountVectorizer`, `Word2Vec`, `LSH`, `KMeans`) dan disimpan dengan nama yang sesuai (misalnya, `model_1_cv`, `model_2_lsh`, dll).

8.  **API → Menyajikan Prediksi dari Model yang Dipilih**
    Sebuah API yang dibangun dengan **Flask** akan memuat salah satu dari tiga set model yang telah dilatih (secara default **Model 3**) dan menyediakan *endpoint* untuk menyajikan hasilnya kepada pengguna.

---

## Dataset yang Digunakan

**Recipe Dataset (over 2M)**
* **Ringkasan Dataset**: Sebuah dataset besar yang berisi lebih dari 2 juta resep yang dikumpulkan dari berbagai sumber online. Untuk proyek ini, kami menggunakan versi yang telah diproses sebelumnya (`recipes_data.csv`) yang berisi kolom-kolom penting.
* **Konten yang Digunakan**:
    -   `title`: Judul dari resep masakan.
    -   `NER` (atau `Cleaned_Ingredients`): Kolom yang berisi daftar bahan-bahan yang sudah dibersihkan dan diekstrak dari teks resep. Kolom ini menjadi dasar utama untuk semua model machine learning.

---

## Cara Menjalankan Sistem

### Langkah-langkah
1.  **Persiapan Dataset**
    Letakkan file dataset `recipes_data.csv` di dalam folder `dataset/` pada direktori proyek.

2.  **Jalankan Seluruh Sistem**
    Buka terminal di direktori proyek dan jalankan perintah:
    ```bash
    docker-compose up --build
    ```
    Proses ini akan memakan waktu cukup lama saat pertama kali dijalankan, terutama pada tahap `spark-trainer`.

3.  **Monitoring Proses**
    Buka terminal baru untuk memantau log dari layanan tertentu:
    ```bash
    # Memantau log proses training model
    docker logs -f recipe_spark_trainer
    
    # Memantau log API setelah training selesai
    docker logs -f recipe_api
    ```

4.  **Menghentikan Sistem**
    Untuk menghentikan semua container, tekan `Ctrl + C` di terminal tempat `docker-compose up` berjalan, atau jalankan perintah ini dari terminal lain:
    ```bash
    docker-compose down
    ```

---

## Dokumentasi API


![Screenshot 2025-06-06 123117](https://github.com/user-attachments/assets/af6f71ef-a9d6-4417-a8c3-123d986eb7d4)

![Screenshot 2025-06-06 123142](https://github.com/user-attachments/assets/fa315d9b-aaed-4ad0-905c-e45a42a798d3)

![Screenshot 2025-06-06 123205](https://github.com/user-attachments/assets/7d66911e-28d2-4eb6-80be-fcb5b964cb1a)


