import os
from threading import Lock
from flask import Flask, request, jsonify
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml import PipelineModel
from pyspark.ml.linalg import SparseVector
import numpy as np

# --- Inisialisasi Flask & Spark ---
app = Flask(__name__)
spark = SparkSession.builder.appName("RecipeAPI").master("local[*]").getOrCreate()
sc = spark.sparkContext

# --- Variabel Global untuk Model & Kontrol Pemuatan ---
_models_loaded = False
_lock = Lock() # Untuk mengunci proses pemuatan agar aman dari race condition

cv_model, vocab, cv_data_broadcast = None, None, None
lsh_model, lsh_data_df = None, None
clustered_data_df = None

def load_models():
    """Fungsi internal untuk memuat semua model dan data."""
    global cv_model, vocab, cv_data_broadcast, lsh_model, lsh_data_df, clustered_data_df

    print("="*50)
    print("MEMUAT MODEL (dijalankan satu kali)...")
    print("="*50)

    MODELS_DIR = os.getenv('MODELS_DIR', '/app/spark_models')
    MODEL_VERSION = "model_3"

    print("Loading recommendation model...")
    cv_model = PipelineModel.load(os.path.join(MODELS_DIR, f"{MODEL_VERSION}_cv"))
    cv_data_df = spark.read.parquet(os.path.join(MODELS_DIR, f"{MODEL_VERSION}_cv_data.parquet")).cache()
    vocab = cv_model.stages[0].vocabulary
    cv_data_broadcast = sc.broadcast(cv_data_df.collect())
    print(f"Recommendation models loaded. Vocabulary size: {len(vocab)}")

    print("Loading LSH model...")
    lsh_model = PipelineModel.load(os.path.join(MODELS_DIR, f"{MODEL_VERSION}_lsh"))
    lsh_data_df = spark.read.parquet(os.path.join(MODELS_DIR, f"{MODEL_VERSION}_lsh_data.parquet")).cache()
    print("LSH models loaded.")

    print("Loading clustering data...")
    clustered_data_df = spark.read.parquet(os.path.join(MODELS_DIR, f"{MODEL_VERSION}_clustered_data.parquet")).cache()
    print("Clustering data loaded.")
    
    print("="*50)
    print("SEMUA MODEL BERHASIL DIMUAT. API SIAP.")
    print("="*50)

@app.before_request
def ensure_models_loaded():
    """
    Dijalankan sebelum setiap request. Menggunakan lock dan flag
    untuk memastikan fungsi load_models() hanya dieksekusi satu kali.
    """
    global _models_loaded
    with _lock:
        if not _models_loaded:
            load_models()
            _models_loaded = True

# --- Fungsi Helper ---
def cosine_similarity(vec1, vec2):
    if isinstance(vec1, SparseVector): vec1 = vec1.toArray()
    if isinstance(vec2, SparseVector): vec2 = vec2.toArray()
    norm_vec1, norm_vec2 = np.linalg.norm(vec1), np.linalg.norm(vec2)
    return np.dot(vec1, vec2) / (norm_vec1 * norm_vec2) if norm_vec1 != 0 and norm_vec2 != 0 else 0.0

# --- Endpoint API ---

@app.route('/recommend-by-ingredients', methods=['POST'])
def recommend_by_ingredients():
    data = request.get_json()
    if not data or 'ingredients' not in data: return jsonify({"error": "JSON body harus berisi key 'ingredients'"}), 400

    input_ingredients = data['ingredients']
    input_indices = [vocab.index(ing) for ing in input_ingredients if ing in vocab]
    if not input_indices: return jsonify({"recommendations": [], "message": "Tidak ada bahan yang dikenali."})
        
    input_vector = SparseVector(len(vocab), sorted(input_indices), [1.0] * len(input_indices))
    similarities = [(row['Title'], float(cosine_similarity(input_vector, row['ingredient_features']))) for row in cv_data_broadcast.value]
    similarities.sort(key=lambda x: x[1], reverse=True)
    return jsonify({"recommendations": [{"title": title, "score": score} for title, score in similarities[:5]]})

@app.route('/find-similar-recipes', methods=['POST'])
def find_similar_recipes():
    data = request.get_json()
    if not data or 'title' not in data: return jsonify({"error": "JSON body harus berisi key 'title'"}), 400

    title_lookup = data['title']
    key_df = lsh_data_df.filter(lsh_data_df.Title == title_lookup).limit(1)
    if key_df.rdd.isEmpty(): return jsonify({"error": f"Resep dengan judul '{title_lookup}' tidak ditemukan."}), 404
        
    key_vector = key_df.select("recipe_vector").first()
    response_df = lsh_model.stages[0].approxNearestNeighbors(lsh_data_df, key_vector.recipe_vector, 6)
    results = response_df.select("Title", "distCol").collect()
    similar_recipes = [{"title": row["Title"], "distance": float(row["distCol"])} for row in results if row["Title"] != title_lookup]
    return jsonify({"source_recipe": title_lookup, "similar_recipes": similar_recipes})

@app.route('/get-recipe-cluster', methods=['POST'])
def get_recipe_cluster():
    data = request.get_json()
    if not data or 'title' not in data: return jsonify({"error": "JSON body harus berisi key 'title'"}), 400

    title_lookup = data['title']
    result = clustered_data_df.filter(clustered_data_df.Title == title_lookup).select("cluster_id").first()
    if not result: return jsonify({"error": f"Resep dengan judul '{title_lookup}' tidak ditemukan."}), 404

    cluster_id = result['cluster_id']
    other_recipes_in_cluster = clustered_data_df.filter((clustered_data_df.cluster_id == cluster_id) & (clustered_data_df.Title != title_lookup)).select("Title").limit(5).collect()
    other_titles = [row['Title'] for row in other_recipes_in_cluster]

    return jsonify({"source_recipe": title_lookup, "cluster_info": {"id": int(cluster_id), "other_recipes_in_cluster": other_titles}})
