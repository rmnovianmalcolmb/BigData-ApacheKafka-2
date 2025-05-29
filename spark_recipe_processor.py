from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
import ast
import os

# --- UDF untuk parsing string list ---
def parse_string_list_udf_func(s_list_str):
    if s_list_str is None or not isinstance(s_list_str, str):
        return []
    try:
        parsed_list = ast.literal_eval(s_list_str)
        if isinstance(parsed_list, list):
            # Pastikan semua item dalam list adalah string
            return [str(item) if item is not None else "" for item in parsed_list]
        return []
    except (ValueError, SyntaxError):
        return []

parse_list_udf = udf(parse_string_list_udf_func, ArrayType(StringType()))

# --- Definisikan Skema untuk Data Resep dari file JSON Lines ---
# Sesuaikan dengan kolom yang dikirim oleh producer_recipe.py
recipe_schema = StructType([
    StructField("title", StringType(), True),
    StructField("ingredients", StringType(), True),
    StructField("directions", StringType(), True),
    StructField("NER", StringType(), True),
    StructField("site", StringType(), True)
    # Tambahkan kolom lain jika producer mengirimnya
])

def build_and_save_model_data(spark_session, input_file_paths, output_parquet_path, model_name):
    print(f"--- Memulai pembangunan data untuk: {model_name} ---")
    if not input_file_paths:
        print(f"Tidak ada file input yang diberikan untuk {model_name}. Melewati.")
        return
    
    print(f"Membaca dari: {input_file_paths}")
    try:
        df_raw = spark_session.read.schema(recipe_schema).json(input_file_paths)
    except Exception as e:
        print(f"Error membaca file untuk {model_name} dari {input_file_paths}: {e}")
        if "Path does not exist" in str(e):
            print("Pastikan file batch mentah sudah ada dan path-nya benar.")
        return

    if df_raw.isEmpty():
        print(f"Tidak ada data yang dibaca untuk {model_name} dari {input_file_paths}. Melewati.")
        return

    print(f"Skema data mentah untuk {model_name} (jumlah baris: {df_raw.count()}):")
    df_raw.printSchema()
    df_raw.show(3, truncate=False)

    df_processed = df_raw \
        .withColumn("NER_parsed_list", parse_list_udf(col("NER"))) \
        .withColumn("ingredients_parsed_list", parse_list_udf(col("ingredients"))) \
        .withColumn("directions_parsed_list", parse_list_udf(col("directions"))) \
        .drop("NER", "ingredients", "directions") # Hapus kolom string asli

    print(f"Skema data setelah diproses untuk {model_name}:")
    df_processed.printSchema()
    df_processed.select("title", "NER_parsed_list").show(3, truncate=True)

    print(f"Menyimpan data {model_name} ke: {output_parquet_path}")
    try:
        df_processed.write.mode("overwrite").parquet(output_parquet_path)
        print(f"Data untuk {model_name} berhasil disimpan.")
    except Exception as e:
        print(f"Error menyimpan data Parquet untuk {model_name} ke {output_parquet_path}: {e}")

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("RecipeDataBatchProcessor") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print("Spark Session untuk Batch Processing Dimulai.")

    RAW_BATCHES_BASE_DIR = "/data_staging/raw_recipe_batches/"
    PROCESSED_MODELS_BASE_DIR = "/data_staging/processed_recipe_models/"

    try:
        # Ambil semua file batch mentah dan urutkan berdasarkan nama (misal, raw_batch_0001.jsonl, ...)
        all_batch_files = sorted([
            os.path.join(RAW_BATCHES_BASE_DIR, f)
            for f in os.listdir(RAW_BATCHES_BASE_DIR)
            if f.startswith("raw_batch_") and f.endswith(".jsonl")
        ])
    except FileNotFoundError:
        print(f"Error: Direktori batch mentah {RAW_BATCHES_BASE_DIR} tidak ditemukan.")
        all_batch_files = []

    if not all_batch_files:
        print("Tidak ada file batch mentah yang ditemukan di {RAW_BATCHES_BASE_DIR}. Keluar.")
        spark.stop()
        exit()
        
    print(f"Ditemukan {len(all_batch_files)} file batch mentah: {all_batch_files[:3]} ...")

    num_total_files = len(all_batch_files)
    
    # Skema B: Kumulatif (1/3 data pertama, 1/3 pertama + 1/3 kedua, semua data)
    # Tentukan jumlah file untuk setiap "sepertiga"
    # Ini adalah perkiraan kasar. Pembagian yang lebih akurat butuh info jumlah record per file.
    files_for_one_third = (num_total_files + 2) // 3 # Pembulatan ke atas untuk mendapatkan cukup file

    # Model 1: 1/3 data pertama
    if num_total_files > 0:
        num_files_model1 = min(files_for_one_third, num_total_files) # Jangan melebihi total file
        model1_input_files = all_batch_files[:num_files_model1]
        build_and_save_model_data(spark, model1_input_files,
                                  os.path.join(PROCESSED_MODELS_BASE_DIR, "model1_first_third_data"),
                                  "Model 1 (First 1/3 Data)")
    else:
        print("Tidak ada file untuk Model 1.")

    # Model 2: 2/3 data pertama (1/3 pertama + 1/3 kedua)
    if num_total_files > 0:
        num_files_model2 = min(files_for_one_third * 2, num_total_files) # Jangan melebihi total file
        model2_input_files = all_batch_files[:num_files_model2]
        build_and_save_model_data(spark, model2_input_files,
                                  os.path.join(PROCESSED_MODELS_BASE_DIR, "model2_first_two_thirds_data"),
                                  "Model 2 (First 2/3 Data)")
    else:
        print("Tidak ada file untuk Model 2.")
        
    # Model 3: Semua data
    if num_total_files > 0:
        build_and_save_model_data(spark, all_batch_files,
                                  os.path.join(PROCESSED_MODELS_BASE_DIR, "model3_all_data"),
                                  "Model 3 (All Data)")
    else:
        print("Tidak ada file untuk Model 3.")

    print("Semua proses pembangunan model selesai.")
    spark.stop()
    print("Spark Session untuk Batch Processing Ditutup.")