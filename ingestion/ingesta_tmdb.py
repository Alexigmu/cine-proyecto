import requests
import json
import os
# Importamos la clase para producir mensajes en Kafka
from kafka import KafkaProducer 
from kafka.errors import KafkaTimeoutError


API_KEY = os.environ.get("TMDB_API_KEY", "TU_API_KEY_POR_DEFECTO")
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

# Constantes de Kafka
TOPIC_NAME = "peliculas_raw" 
# Obtenemos la dirección interna del broker desde la variable de entorno de Docker Compose
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")

def main():
    # 1) Llamar a TMDb
    resp = requests.get(URL, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    peliculas = data.get("results", [])

   # 2. Conectar al Productor de Kafka
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            # Serializador: Kafka solo acepta bytes. Convertimos el dict a JSON (string) y luego a bytes (utf-8)
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            # Usamos el ID como clave para que los mensajes relacionados vayan a la misma partición (si tuvieras más)
            key_serializer=lambda k: str(k).encode('utf-8') 
        )
    except Exception as e:
        print(f"ERROR: No se pudo conectar a Kafka en {KAFKA_BROKER}. Asegúrate de que los contenedores estén levantados.")
        print(e)
        return

    mensajes_enviados = 0
    print(f"Conectado a Kafka Broker en: {KAFKA_BROKER}. Enviando a Topic: {TOPIC_NAME}")
    
    # 3. Enviar cada película al Topic de Kafka
    for p in peliculas:
        doc = seleccionar_campos(p)
        
        try:
            # Enviar el mensaje: key es el ID de la película, el value es el documento
            # Usamos el ID de la película como clave (key)
            producer.send(
                TOPIC_NAME, 
                key=doc["id"], 
                value=doc
            )
            mensajes_enviados += 1
        except KafkaTimeoutError:
            print(f"Advertencia: El mensaje para la película ID {doc['id']} ha expirado. Intentando continuar...")
        
    # Esperar a que todos los mensajes pendientes se envíen
    producer.flush() 
    producer.close()
    
    print("-" * 50)
    print(f"Proceso de ingesta finalizado. {mensajes_enviados} mensajes enviados a Kafka Topic: {TOPIC_NAME}.")
    print("-" * 50)


if __name__ == "__main__":
    main()