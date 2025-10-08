import requests
import json
import os
import time
from kafka import KafkaProducer

API_KEY = os.environ.get("TMDB_API_KEY", "TU_CLAVE_API_AQUI") 
BASE_URL = "https://api.themoviedb.org/3/discover/movie" 
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:29092") 
TOPIC_NAME = "peliculas_raw"

PAGES_CALIDAD = 50 # Objetivo: 1000 películas con muchos votos para asegurar películas de calidad.
PARAMS_CALIDAD = {
    "api_key": API_KEY,
    "language": "es-ES",
    "sort_by": "vote_count.desc", # Priorizar las más votadas
    "vote_count.gte": 1000,        # Solo incluir si tienen al menos 1000 votos
}

PAGES_ACTUALIDAD = 5 # Solo las primeras 5 páginas de popularidad reciente (100 películas)
# Ajusta la fecha aquí. Ejemplo: '2025-01-01' para películas estrenadas este año
FECHA_MINIMA_ESTRENO = "2024-01-01" 
PARAMS_ACTUALIDAD = {
    "api_key": API_KEY,
    "language": "es-ES",
    "sort_by": "popularity.desc", # Priorizar las más populares
    "primary_release_date.gte": FECHA_MINIMA_ESTRENO, # Películas recientes
    # Importante: Quitamos el filtro de vote_count.gte para permitir películas nuevas
}


def seleccionar_campos(pelicula: dict) -> dict: 
    """Devuelve solo los campos que hemos decidido guardar."""
    release_date = pelicula.get("release_date", "1900-01-01") 
    
    return {
        "id": pelicula.get("id"),
        "title": pelicula.get("title"),
        "original_title": pelicula.get("original_title"),
        "overview": pelicula.get("overview"),
        "release_date": release_date,
        "release_year": int(release_date.split('-')[0]) if '-' in release_date else None,
        "genre_ids": pelicula.get("genre_ids", []),
        "vote_average": pelicula.get("vote_average"),
        "popularity": pelicula.get("popularity"),
        "poster_path": pelicula.get("poster_path"),
        "backdrop_path": pelicula.get("backdrop_path"),
        "original_language": pelicula.get("original_language"),
        # Campo para el CineDuel que la API usará para el ranking
        "duelos_ganados": 0 
    }


def fetch_and_produce(producer, pages_to_fetch, base_params, strategy_name):
    """Función genérica para iterar sobre páginas y enviar a Kafka."""
    total_peliculas_enviadas = 0
    
    for page in range(1, pages_to_fetch + 1):
        params = base_params.copy()
        params["page"] = page
        
        try:
            resp = requests.get(BASE_URL, params=params, timeout=15)
            resp.raise_for_status() 
            data = resp.json()
            peliculas = data.get("results", [])
            total_pages_api = data.get("total_pages", 1)

            if not peliculas:
                print(f"[INFO - {strategy_name}] Página {page} vacía. Fin de la ingesta.")
                break
                
            for p in peliculas:
                doc = seleccionar_campos(p)
                producer.send(TOPIC_NAME, value=doc)
                total_peliculas_enviadas += 1

            print(f"[{strategy_name}] Página {page}/{min(pages_to_fetch, total_pages_api)} procesada. Enviadas: {total_peliculas_enviadas}")

            # Parada para evitar sobrecargar la API de TMDb
            time.sleep(0.5) 
            
            if page >= total_pages_api:
                print(f"[INFO - {strategy_name}] Alcanzada la última página disponible de TMDb.")
                break

        except requests.exceptions.RequestException as e:
            print(f"[ERROR TMDb - {strategy_name}] Error al obtener datos de la página {page}: {e}")
            break

    return total_peliculas_enviadas

def main():
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print(f"[PRODUCTOR] Conectado al broker Kafka: {KAFKA_BROKER}")

    except Exception as e:
        print(f"[ERROR FATAL] No se pudo conectar al broker Kafka: {e}")
        return

    print("==============================================")
    print("CALIDAD Y CANTIDAD (Base Histórica)")
    print("==============================================")
    enviadas_calidad = fetch_and_produce(producer, PAGES_CALIDAD, PARAMS_CALIDAD, "CALIDAD")
    
    print("\n==============================================")
    print("ACTUALIDAD (Películas Recientes)")
    print("==============================================")
    enviadas_actualidad = fetch_and_produce(producer, PAGES_ACTUALIDAD, PARAMS_ACTUALIDAD, "ACTUALIDAD")

    # Sincronizar y cerrar
    producer.flush() 
    total_final = enviadas_calidad + enviadas_actualidad
    print(f"\n[FINAL] Tarea completada. Total de películas enviadas a Kafka: {total_final}.")

if __name__ == '__main__':
    main()