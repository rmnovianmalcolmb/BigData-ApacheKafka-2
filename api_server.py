from flask import Flask, request, jsonify
import pandas as pd
import os
import ast
import numpy as np # Untuk isinstance(value, np.ndarray) dan np.generic

app = Flask(__name__)

MODEL_DATA_BASE_DIR = '/data_staging/processed_recipe_models/'

MODEL_MAP = {
    "model1": "model1_first_third_data",
    "model2": "model2_first_two_thirds_data",
    "model3": "model3_all_data"
}

MODEL_CACHE = {}

def load_model_data(model_name_key):
    actual_model_name = MODEL_MAP.get(model_name_key)
    if not actual_model_name:
        print(f"Kunci model tidak valid: {model_name_key}")
        return None
    
    if actual_model_name in MODEL_CACHE:
        print(f"Menggunakan data dari cache untuk {actual_model_name}")
        return MODEL_CACHE[actual_model_name]

    path = os.path.join(MODEL_DATA_BASE_DIR, actual_model_name)
    try:
        if os.path.exists(path) and os.path.isdir(path):
            df = pd.read_parquet(path)
            print(f"Data untuk {actual_model_name} berhasil dimuat. Jumlah baris: {len(df)}")
            
            required_parsed_list_columns = ['NER_parsed_list', 'ingredients_parsed_list', 'directions_parsed_list']
            original_string_columns = {'NER_parsed_list': 'NER', 
                                       'ingredients_parsed_list': 'ingredients', 
                                       'directions_parsed_list': 'directions'}

            for col_name in required_parsed_list_columns:
                if col_name not in df.columns:
                    print(f"Peringatan: Kolom '{col_name}' tidak ditemukan di {actual_model_name}.")
                    original_col_key = original_string_columns.get(col_name)
                    if original_col_key and original_col_key in df.columns:
                        print(f"Mencoba parse kolom '{original_col_key}' string di API untuk '{col_name}'...")
                        def safe_literal_eval_api(s_list_str):
                            if s_list_str is None or not isinstance(s_list_str, str): return []
                            try:
                                parsed = ast.literal_eval(s_list_str)
                                return [str(item) if item is not None else "" for item in parsed] if isinstance(parsed, list) else []
                            except: return []
                        df[col_name] = df[original_col_key].apply(safe_literal_eval_api)
                    else:
                        print(f"Membuat kolom '{col_name}' kosong untuk {actual_model_name}.")
                        df[col_name] = pd.Series([[] for _ in range(len(df))], index=df.index)
            
            MODEL_CACHE[actual_model_name] = df
            return df
        else:
            print(f"Path data model tidak ditemukan atau bukan direktori Parquet: {path}")
            return pd.DataFrame()
    except Exception as e:
        print(f"Error memuat data model {actual_model_name} dari {path}: {e}")
        return pd.DataFrame()

def clean_ingredient_name_api(ingredient_name):
    return str(ingredient_name).lower().strip()

def get_recipe_recommendations_from_df_api(user_ingredients, recipes_df, top_n=3):
    print("--- FUNGSI: get_recipe_recommendations_from_df_api DIPANGGIL ---")

    if recipes_df.empty or 'NER_parsed_list' not in recipes_df.columns:
        print("DataFrame resep kosong atau kolom NER_parsed_list tidak ada.")
        return pd.DataFrame()

    cleaned_user_ingredients = set([clean_ingredient_name_api(ing) for ing in user_ingredients])
    recommendations_data = [] 

    print(f"--- DEBUG GLOBAL (get_recipe_recommendations_from_df_api) ---")
    print(f"User Ingredients (setelah dibersihkan): {cleaned_user_ingredients}")
    print(f"Akan memproses {len(recipes_df)} resep.")
    print(f"--- END DEBUG GLOBAL ---")

    for index, row in recipes_df.iterrows():
        recipe_title = row.get('title', 'Tanpa Judul')
        ner_list_from_df = row.get('NER_parsed_list') 

        recipe_ner_ingredients_set = set()
        iterable_ner_list = []
        if isinstance(ner_list_from_df, np.ndarray):
            iterable_ner_list = ner_list_from_df.tolist()
        elif isinstance(ner_list_from_df, list):
            iterable_ner_list = ner_list_from_df
        
        if iterable_ner_list: 
            for ing_from_df in iterable_ner_list:
                if ing_from_df is not None: 
                    recipe_ner_ingredients_set.add(clean_ingredient_name_api(ing_from_df))
        
        target_title_debug = "jewell ball's chicken" 
        current_recipe_title_normalized = recipe_title.lower().strip()

        if current_recipe_title_normalized == target_title_debug: # Debug print (bisa dikomentari jika sudah OK)
            print(f"\n--- MEMERIKSA RESEP TARGET: '{recipe_title}' (Row Index: {index}) ---")
            print(f"1. NER_parsed_list MENTAH dari DataFrame: {ner_list_from_df} (Tipe: {type(ner_list_from_df)})")
            print(f"2. Bahan Resep (NER) SETELAH DIBERSIHKAN: {recipe_ner_ingredients_set}")
            current_common_ingredients = cleaned_user_ingredients.intersection(recipe_ner_ingredients_set)
            print(f"3. Common Ingredients (Irisan dengan NER): {current_common_ingredients}")
            current_intersection_size = len(current_common_ingredients)
            print(f"4. Intersection Size (dengan NER): {current_intersection_size}")
            raw_ingredients_parsed_list_debug = row.get('ingredients_parsed_list')
            raw_directions_parsed_list_debug = row.get('directions_parsed_list')
            print(f"DEBUG: Raw ingredients_parsed_list dari row: {raw_ingredients_parsed_list_debug} (Tipe: {type(raw_ingredients_parsed_list_debug)})")
            print(f"DEBUG: Raw directions_parsed_list dari row: {raw_directions_parsed_list_debug} (Tipe: {type(raw_directions_parsed_list_debug)})")
            if current_intersection_size > 0: print(f"   ==> Resep INI SEHARUSNYA dipertimbangkan (berdasarkan NER).")
            else: print(f"   ==> Resep INI TIDAK AKAN dipertimbangkan (intersection NER adalah 0).")
            print(f"--- AKHIR PEMERIKSAAN RESEP TARGET ---\n")

        if not recipe_ner_ingredients_set: 
            continue

        common_ingredients_with_ner = cleaned_user_ingredients.intersection(recipe_ner_ingredients_set)
        intersection_size_with_ner = len(common_ingredients_with_ner)
        
        if intersection_size_with_ner > 0: 
            # --- PERBAIKAN UNTUK ingredients DAN directions DARI ndarray ---
            raw_ingredients = row.get('ingredients_parsed_list')
            if isinstance(raw_ingredients, np.ndarray):
                final_ingredients_for_display = raw_ingredients.tolist()
            elif isinstance(raw_ingredients, list):
                final_ingredients_for_display = raw_ingredients
            else:
                final_ingredients_for_display = [] 

            raw_directions = row.get('directions_parsed_list')
            if isinstance(raw_directions, np.ndarray):
                final_directions_for_display = raw_directions.tolist()
            elif isinstance(raw_directions, list):
                final_directions_for_display = raw_directions
            else:
                final_directions_for_display = []
            # --- AKHIR PERBAIKAN ---
            
            union_size_with_ner = len(cleaned_user_ingredients.union(recipe_ner_ingredients_set))
            jaccard_score_ner = intersection_size_with_ner / union_size_with_ner if union_size_with_ner > 0 else 0
            user_coverage_score_ner = intersection_size_with_ner / len(cleaned_user_ingredients) if cleaned_user_ingredients else 0

            recommendations_data.append({
                'title': recipe_title,
                'ingredients': final_ingredients_for_display,
                'directions': final_directions_for_display,
                '_jaccard_ner': jaccard_score_ner, 
                '_matched_count_ner': intersection_size_with_ner,
                '_coverage_ner': user_coverage_score_ner 
            })
    
    sorted_recommendations_data = sorted(
        recommendations_data, 
        key=lambda x: (x['_jaccard_ner'], x['_matched_count_ner'], x['_coverage_ner']), 
        reverse=True
    )
    
    final_recommendations_list = []
    for rec_dict in sorted_recommendations_data:
        final_recommendations_list.append({
            'title': rec_dict['title'],
            'ingredients': rec_dict['ingredients'],
            'directions': rec_dict['directions']
        })
        
    return pd.DataFrame(final_recommendations_list).head(top_n)

@app.route('/recommend/<model_version>', methods=['POST'])
def recommend_by_model(model_version):
    print(f"--- ENDPOINT /recommend/{model_version} DIPANGGIL ---")
    if model_version not in MODEL_MAP:
        return jsonify({"error": "Versi model tidak valid. Gunakan model1, model2, atau model3."}), 400

    recipes_df = load_model_data(model_version) 
    if recipes_df is None or recipes_df.empty: 
        return jsonify({"error": f"Data untuk model {model_version} tidak bisa dimuat atau kosong."}), 500

    try:
        data = request.get_json()
        if not data or 'ingredients' not in data or not isinstance(data['ingredients'], list):
            return jsonify({"error": "Input JSON tidak valid. Butuh {'ingredients': ['bahan1', 'bahan2']}"}), 400
        
        user_ingredients_input = data['ingredients']
        if not user_ingredients_input: 
             return jsonify({"error": "Daftar bahan tidak boleh kosong."}), 400

        recommendations_df = get_recipe_recommendations_from_df_api(user_ingredients_input, recipes_df, top_n=3)
        
        if recommendations_df.empty:
            return jsonify({"message": "Tidak ada rekomendasi yang cocok untuk bahan yang diberikan.", "recommendations": []})
            
        return jsonify({
            "model_used": model_version,
            "user_ingredients": user_ingredients_input,
            "recommendations": recommendations_df.to_dict(orient='records')
        })
    except Exception as e:
        print(f"Error di endpoint /recommend: {e}") 
        return jsonify({"error": f"Terjadi kesalahan pada server: {str(e)}"}), 500

@app.route('/recipe_details/<model_version>/<path:recipe_title>', methods=['GET'])
def get_recipe_details_endpoint(model_version, recipe_title):
    print(f"--- ENDPOINT /recipe_details/{model_version}/{recipe_title} DIPANGGIL ---")
    if model_version not in MODEL_MAP:
        return jsonify({"error": "Versi model tidak valid"}), 400

    recipes_df = load_model_data(model_version) 
    if recipes_df is None or recipes_df.empty: 
        return jsonify({"error": f"Data untuk model {model_version} tidak bisa dimuat atau kosong."}), 500

    if not recipe_title:
        return jsonify({"error": "Judul resep tidak boleh kosong"}), 400
        
    recipe_detail_series = recipes_df[recipes_df['title'].str.lower().str.strip() == recipe_title.lower().strip()]
    
    if recipe_detail_series.empty:
        return jsonify({"error": f"Resep dengan judul '{recipe_title}' tidak ditemukan di model {model_version}"}), 404
    
    recipe_dict_raw = recipe_detail_series.iloc[0].to_dict()
    
    sanitized_recipe_dict = {}
    for key, value in recipe_dict_raw.items():
        if isinstance(value, np.ndarray):
            sanitized_recipe_dict[key] = value.tolist()
        elif isinstance(value, list):
            new_list = []
            for item in value:
                if isinstance(item, np.generic): 
                    new_list.append(item.item()) 
                elif pd.isna(item): 
                    new_list.append(None) 
                else:
                    new_list.append(str(item)) 
            sanitized_recipe_dict[key] = new_list
        elif pd.isna(value): 
             sanitized_recipe_dict[key] = None 
        else:
            sanitized_recipe_dict[key] = value
    
    return jsonify(sanitized_recipe_dict)

@app.route('/model_stats/<model_version>', methods=['GET'])
def get_model_stats_endpoint(model_version):
    print(f"--- ENDPOINT /model_stats/{model_version} DIPANGGIL ---")
    if model_version not in MODEL_MAP:
        return jsonify({"error": "Versi model tidak valid"}), 400

    recipes_df = load_model_data(model_version) 
    if recipes_df is None or recipes_df.empty: 
        return jsonify({"error": f"Data untuk model {model_version} tidak bisa dimuat atau kosong."}), 500

    total_recipes = len(recipes_df)
    
    all_ner_ingredients = []
    if 'NER_parsed_list' in recipes_df.columns:
        for ner_list in recipes_df['NER_parsed_list'].dropna(): 
            if isinstance(ner_list, list):
                 all_ner_ingredients.extend([clean_ingredient_name_api(ing) for ing in ner_list if ing is not None]) 
    
    from collections import Counter
    common_ingredients_counts = Counter(all_ner_ingredients).most_common(10)

    return jsonify({
        "model_name": MODEL_MAP.get(model_version, "Nama Model Tidak Diketahui"), 
        "total_recipes_in_model": total_recipes,
        "most_common_NER_ingredients": common_ingredients_counts
    })

if __name__ == '__main__':
    print("--- API SERVER MULAI (if __name__ == '__main__') ---")
    app.run(host='0.0.0.0', port=5000, debug=True)