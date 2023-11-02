# API de Análisis de Datos de Juegos de Steam

Este proyecto consiste en una API desarrollada con **FastAPI** para realizar análisis y consultas de datos relacionados con juegos de la plataforma Steam. La API se conecta a una base de datos SQLite y utiliza archivos Parquet para almacenar y procesar información. Permite realizar operaciones de almacenamiento, análisis de datos y recomendaciones de juegos.

## Endpoints Principales

### Documentación de la API

- **Descripción:** Muestra la documentación para los endpoints disponibles.

    **Endpoint:** `/` [GET]
    
### Almacenamiento de Datos

- **Endpoint:** `/StorePlayTimeGenre` [GET]
- **Descripción:** Almacena datos de tiempo de juego por género en la base de datos.

- **Endpoint:** `/StoreSteamGames` [GET]
- **Descripción:** Almacena datos de juegos de Steam en la base de datos.

- **Endpoint:** `/StoreUserItems` [GET]
- **Descripción:** Almacena datos de elementos de usuarios en la base de datos.

- **Endpoint:** `/StoreUserReviews` [GET]
- **Descripción:** Almacena datos de reseñas de usuarios en la base de datos.

### Consultas y Análisis

- **Endpoint:** `/PlayTimeGenre/{genre}` [GET]
- **Descripción:** Obtiene el año de lanzamiento con más horas jugadas para un género específico.

- **Endpoint:** `/UserForGenre/{genre}` [GET]
- **Descripción:** Obtiene al usuario con más horas jugadas para un género específico.

- **Endpoint:** `/UsersRecommend/{year}` [GET]
- **Descripción:** Obtiene los 3 juegos más recomendados para un año específico.

- **Endpoint:** `/UsersNotRecommend/{year}` [GET]
- **Descripción:** Obtiene los 3 juegos menos recomendados para un año específico.

- **Endpoint:** `/SentimentAnalysis/{year}` [GET]
- **Descripción:** Realiza análisis de sentimiento para un año específico.

- **Endpoint:** `/RecommendedGames/{id}` [GET]
- **Descripción:** Obtiene juegos similares basados en un juego específico.

## ETL - Procesos de Extracción, Transformación y Carga

El proyecto incluye una serie de procesos ETL que se encargan de cargar, transformar y almacenar datos para su posterior análisis. A continuación, se describen los ETL realizados:

- **ETL output_steam_games.json:** Proceso de limpieza y transformación del archivo JSON de juegos de Steam para generar archivos Parquet.

- **ETL australian_users_items.json:** Proceso de normalización y división del archivo JSON de elementos de usuarios para generar archivos Parquet.

- **ETL australian_user_reviews.csv:** Proceso de análisis de sentimiento y transformación del archivo CSV de reseñas de usuarios para generar archivos Parquet.

- **Generación de Modelos:** Creación de archivos Parquet para modelado de juegos y análisis de similitud.

## Notas

El proyecto hace uso de múltiples bibliotecas y tecnologías, tales como FastAPI para la creación de la API, Pandas para el manejo de datos, SQLite para la base de datos, entre otras.