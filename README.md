
# Cine Proyecto 🎬

Proyecto de prácticas: ingesta de datos de The Movie Database (TMDb), almacenamiento en MongoDB, API REST en Node/Express

## 🚀 Cómo ejecutar

1. Levantar la base de datos y mongo-express:
   ```bash
   docker compose up -d

2. Ingerir películas trending:
   ```bash
    python ingestion/ingesta_tmdb.py

--> Endpoints previstos (MVP)

GET /movies/trending → lista películas ordenadas por popularidad.

GET /movies/:id → detalle de película.

GET /movies/recommend?year=&genre=&min_vote= → recomendación filtrada.

--> Roadmap

V1 (MVP): Trending + Recomendador.

V2: Favoritos y Watchlist.

V3: Batalla de películas, estrenos y visualizaciones.
