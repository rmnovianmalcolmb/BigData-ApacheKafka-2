import os
import ast
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import ArrayType, StringType
from pyspark.ml.feature import CountVectorizer, Word2Vec, BucketedRandomProjectionLSH
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline

SPARK_MASTER_URL = os.getenv('SPARK_MASTER_URL')
APP_NAME = os.getenv('APP_NAME')
DATA_BATCHES_DIR = os.getenv('DATA_BATCHES_DIR')
MODELS_DIR = os.getenv('MODELS_DIR')

os.makedirs(MODELS_DIR, exist_ok=True)

spark = SparkSession.builder.appName(APP_NAME).master(SPARK_MASTER_URL).getOrCreate()

def parse_string_to_list(text_list):
    if not text_list: return []
    try: return ast.literal_eval(text_list)
    except (ValueError, SyntaxError): return []

parse_udf = udf(parse_string_to_list, ArrayType(StringType()))

def train_models_on_batches(batch_files, model_name_suffix):
    model_identifier = f"model_{model_name_suffix}"
    print(f"\n--- Training {model_identifier} using {len(batch_files)} batch(es) ---")
    
    full_data_path = [os.path.join(DATA_BATCHES_DIR, f) for f in batch_files]
    df = spark.read.option("header", "true").csv(full_data_path).na.drop(subset=["Title", "Cleaned_Ingredients"])
    
    df_transformed = df.withColumn("ingredients_arr", parse_udf(col("Cleaned_Ingredients")))
    df_clean = df_transformed.filter(col("ingredients_arr").isNotNull()) \
                             .filter(col("ingredients_arr").getItem(0).isNotNull())

    print(f"Total rows after cleaning for model {model_name_suffix}: {df_clean.count()}")

    min_df_threshold = 2
    min_count_threshold = 2

    cv = CountVectorizer(inputCol="ingredients_arr", outputCol="ingredient_features", minDF=min_df_threshold)
    cv_model = Pipeline(stages=[cv]).fit(df_clean)
    vocab_size = len(cv_model.stages[0].vocabulary)
    print(f"CountVectorizer vocabulary size for model {model_name_suffix}: {vocab_size}")
    if vocab_size == 0:
        print("ERROR: Vocabulary size is 0. Aborting.")
        return

    cv_model_path = os.path.join(MODELS_DIR, model_identifier + "_cv")
    cv_model.write().overwrite().save(cv_model_path)
    vectorized_data_for_api = cv_model.transform(df_clean).select("Title", "ingredient_features")
    vectorized_data_path = os.path.join(MODELS_DIR, model_identifier + "_cv_data.parquet")
    vectorized_data_for_api.write.mode("overwrite").parquet(vectorized_data_path)
    print("CountVectorizer model and data saved.")

    word2vec = Word2Vec(vectorSize=100, minCount=min_count_threshold, inputCol="ingredients_arr", outputCol="recipe_vector")
    w2v_model = Pipeline(stages=[word2vec]).fit(df_clean)
    w2v_model_path = os.path.join(MODELS_DIR, model_identifier + "_w2v")
    w2v_model.write().overwrite().save(w2v_model_path)
    print("Word2Vec model saved.")
    
    recipe_vectors_df = w2v_model.transform(df_clean)

    lsh = BucketedRandomProjectionLSH(inputCol="recipe_vector", outputCol="hashes", bucketLength=2.0, numHashTables=3)
    lsh_model = Pipeline(stages=[lsh]).fit(recipe_vectors_df)
    lsh_model_path = os.path.join(MODELS_DIR, model_identifier + "_lsh")
    lsh_model.write().overwrite().save(lsh_model_path)
    print("LSH model saved.")
    
    lsh_data_path = os.path.join(MODELS_DIR, model_identifier + "_lsh_data.parquet")
    recipe_vectors_df.select("Title", "recipe_vector").write.mode("overwrite").parquet(lsh_data_path)

    print("Caching dataframe before KMeans training...")
    recipe_vectors_df.cache()
    
    kmeans = KMeans(featuresCol="recipe_vector", k=20, seed=42, maxIter=20)
    kmeans_model = Pipeline(stages=[kmeans]).fit(recipe_vectors_df)
    kmeans_model_path = os.path.join(MODELS_DIR, model_identifier + "_kmeans")
    kmeans_model.write().overwrite().save(kmeans_model_path)
    print("KMeans model saved.")
    
    clustered_data = kmeans_model.transform(recipe_vectors_df).select("Title", "prediction")
    clustered_data_path = os.path.join(MODELS_DIR, model_identifier + "_clustered_data.parquet")
    clustered_data.withColumnRenamed("prediction", "cluster_id").write.mode("overwrite").parquet(clustered_data_path)
    print("Clustered data saved.")
    
    recipe_vectors_df.unpersist()

train_models_on_batches(["batch_0.csv"], "1")
train_models_on_batches(["batch_0.csv", "batch_1.csv"], "2")
train_models_on_batches(["batch_0.csv", "batch_1.csv", "batch_2.csv"], "3")

print("\nAll models trained and saved successfully.")
spark.stop()
