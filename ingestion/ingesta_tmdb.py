import requests
import pymongo
import os

API_KEY = os.environment.get("TMDB_API_KEY", "TU_API_KEY_POR_DEFECTO")
URL = f"https://api.themoviedb.org/3/trending/movie/week?api_key={API_KEY}"

def seleccionar_campos(pelicula: dict) -> dict: #Devuelve solo los campos que hemos decidido guardar
    return {
        "id": pelicula.get("id"),
        "title": pelicula.get("title"),
        "original_title": pelicula.get("original_title"),
        "overview": pelicula.get("overview"),
        "release_date": pelicula.get("release_date"),
        "genre_ids": pelicula.get("genre_ids", []),
        "vote_average": pelicula.get("vote_average"),
        "popularity": pelicula.get("popularity"),
        "poster_path": pelicula.get("poster_path"),
        "backdrop_path": pelicula.get("backdrop_path"),
        "original_language": pelicula.get("original_language"),
    }

def main():
    # 1) Llamar a TMDb
    resp = requests.get(URL, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    peliculas = data.get("results", [])

    # 2) Conectar a MongoDB
    cliente = pymongo.MongoClient("mongodb://mongo:27017")
    db = cliente["cine"]
    coleccion = db["peliculas"]

    # Crear índice único por id (si ya existe, no pasa nada)
    try:
        coleccion.create_index("id", unique=True)
    except Exception:
        pass

    # 3) Upsert: si existe -> reemplaza, si no -> inserta
    upserts = 0
    for p in peliculas:
        doc = seleccionar_campos(p)
        if doc["id"] is None:
            continue
        res = coleccion.replace_one({"id": doc["id"]}, doc, upsert=True)
        if res.upserted_id is not None or res.modified_count == 1:
            upserts += 1

    print(f"Películas procesadas: {len(peliculas)} | insertadas/actualizadas: {upserts}")

if __name__ == "__main__":
    main()
