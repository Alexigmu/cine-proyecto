import os
import time
import json
import requests
from typing import Dict, List, Tuple
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError

# ==============================
# Configuración (env / defaults)
# ==============================
API_KEY   = os.environ.get("TMDB_API_KEY", "TU_CLAVE_API_AQUI")
BASE_URL  = "https://api.themoviedb.org/3/discover/movie"
MONGO_URL = os.environ.get("MONGO_URL", "mongodb://mongo:27017")
MONGO_DB  = os.environ.get("MONGO_DB", "cine")
COLL_NAME = "peliculas"

# =========================
# Estrategias de recopilación
# =========================
PAGES_RATED = 50  # objetivo: ~1000 pelis mejor valoradas con mínimo de votos
PARAMS_RATED = {
    "api_key": API_KEY,
    "language": "es-ES",
    "sort_by": "vote_average.desc",  # prioriza las mejor valoradas
    "vote_count.gte": 1000, # solo si tienen al menos 1000 votos
    "vote_average.gte": 6.0,    # solo si tienen al menos 6.0 de rating    
}

PAGES_ACTUALIDAD = 5   # primeras 5 páginas de popularidad reciente (~100 pelis)
FECHA_MINIMA_ESTRENO = "2024-01-01"
PARAMS_ACTUALIDAD = {
    "api_key": API_KEY,
    "language": "es-ES",
    "sort_by": "popularity.desc",             # prioriza populares
    "primary_release_date.gte": FECHA_MINIMA_ESTRENO,  # recientes
    "vote_average.gte": 6.0, # solo si tienen al menos 6.0 rating
    # sin filtro de vote_count para permitir estrenos
}

# =========================
# Transformación
# =========================
def seleccionar_campos(pelicula: Dict) -> Dict:
    """Reduce el documento TMDb a los campos que queremos guardar."""
    release_date = pelicula.get("release_date", "1900-01-01")
    # Se guarda en una variable antes, asignando un valor por defecto. Luego usaremos esta variable, 3 veces y así es más claro.
    return {
        "id": pelicula.get("id"),
        "title": pelicula.get("title"),
        "original_title": pelicula.get("original_title"),
        "overview": pelicula.get("overview"),
        "release_date": release_date,
        "release_year": int(release_date.split("-")[0]) if "-" in release_date else None, # Saca el año en función del release_date
        "genre_ids": pelicula.get("genre_ids", []), # Como pueden ser varios añadimos [] para que sea vacío si no hay géneros
        "vote_average": pelicula.get("vote_average"),
        "popularity": pelicula.get("popularity"),
        "poster_path": pelicula.get("poster_path"),
        "backdrop_path": pelicula.get("backdrop_path"),
        "original_language": pelicula.get("original_language"),
        # Campo para ranking en CineDuel
        "duelos_ganados": 0,
    }

# =========================
# Ingesta + upserts a Mongo
# =========================
def fetch_page(params: Dict) -> Tuple[List[Dict], int]:
# params: Dict → el argumento params debe ser un diccionario con los parámetros que se enviarán a la API (por ejemplo, api_key, language, sort_by…).
# -> Tuple[List[Dict], int] → la función devuelve una tupla, es decir, dos valores:
# Una lista de diccionarios (List[Dict]) → cada diccionario representa una película.
# Un número entero (int) → el total de páginas que devuelve la API.
    """Pide una página a TMDb y devuelve (lista_peliculas, total_pages_api)."""
    resp = requests.get(BASE_URL, params=params, timeout=15) # Hace una petición HTTP a TMDb
    resp.raise_for_status() # Control de errores
    data = resp.json() # Convierte la respuesta JSON en un diccionario de Python.
    return data.get("results", []), int(data.get("total_pages", 1)) # Devuelve lista de pelis (results) y nº de págs (total_pages)


def fetch_and_upsert(collection, pages_to_fetch: int, base_params: Dict, strategy_name: str) -> int:
    """Itera páginas de TMDb, transforma y upserta cada película por 'id'."""
    total_upserts = 0 # Contamos cuántas películas guardamos/actualizamos para devolverlo al final.

    for page in range(1, pages_to_fetch + 1):
        params = base_params.copy()
        params["page"] = page

        try:
            peliculas, total_pages_api = fetch_page(params)
        except requests.exceptions.RequestException as e: # Si hay un error para y lo devuelve
            print(f"[ERROR TMDb - {strategy_name}] Página {page}: {e}")
            break

        if not peliculas: # Si ya no hay más películas en la búsqueda, avisa de página vacía y FIN.
            print(f"[INFO - {strategy_name}] Página {page} vacía. Fin.")
            break

        for p in peliculas: # Recorremos cada película devuelta por TMDB
            doc = seleccionar_campos(p) # Seleccionamos solo los campos que nos interesan
            if doc.get("id") is None: 
                continue  # descarta registros sin id TMDb

            # upsert por id
            res = collection.update_one(
                {"id": doc["id"]},
                {"$set": doc},
                upsert=True
            ) # Si ya tenemos la película, la actualiza, si no, la inserta.
            # Contabilizamos si insertó o actualizó algo
            if res.upserted_id is not None or res.modified_count > 0:
                total_upserts += 1 # Vamos actualizando el total_upserts por cada insert o actualización.

        print(f"[{strategy_name}] Página {page}/{min(pages_to_fetch, total_pages_api)} procesada. Upserts acumulados: {total_upserts}")

        # Rate limit o Throttling para respetar API TMDb
        time.sleep(0.5) # Eete una pausa de medio segundo entre páginas, para evitar bloqueos de acceso.

        if page >= total_pages_api: # Si alcanzamos la última página, salimos del bucle sin tener que seguir buscando.
            print(f"[INFO - {strategy_name}] Alcanzada la última página disponible en TMDb ({total_pages_api}).")
            break

    return total_upserts

# =========================
# Main
# =========================
def main():
    # 1) Conexión a Mongo
    client = MongoClient(MONGO_URL)
    db = client[MONGO_DB]
    coll = db[COLL_NAME]

    # 2) Índice único por 'id' (si no existe)
    #    Evita duplicados y acelera el upsert
    try:
        coll.create_index([("id", ASCENDING)], name="uniq_tmdb_id", unique=True)
    except Exception as e:
        print(f"[WARN] No se pudo crear índice único (puede existir): {e}")

    print(f"[MONGO] Conectado a {MONGO_URL}/{MONGO_DB}, colección '{COLL_NAME}'")

    # 3) Estrategia 1 — TOP Rated
    print("=" * 46)
    print("TOP RATED (Base Histórica)")
    print("=" * 46)
    upserts_calidad = fetch_and_upsert(coll, PAGES_RATED, PARAMS_RATED, "TOP RATED")

    # 4) Estrategia 2 — Actualidad
    print("\n" + "=" * 46) # Decoración en consola
    print("ACTUALIDAD (Películas Recientes)")
    print("=" * 46)
    upserts_actualidad = fetch_and_upsert(coll, PAGES_ACTUALIDAD, PARAMS_ACTUALIDAD, "ACTUALIDAD")

    total = upserts_calidad + upserts_actualidad
    print(f"\n[FINAL] Tarea completada. Total de upserts aplicados: {total}")
    import datetime
    print(f"[FINAL] Fecha y hora de ejecución: {datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S')}")

if __name__ == "__main__": # Solo ejecuta main() si este archivo se ejecuta directamente, no si alguien lo importa.
    main()