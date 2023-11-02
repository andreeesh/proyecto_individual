from fastapi import FastAPI
import pandas as pd
import sqlite3
import pyarrow.parquet as pq
import os
from fastapi.responses import HTMLResponse

app = FastAPI()


@app.get("/", response_class=HTMLResponse)
async def doc():
    html = """
    <!DOCTYPE html>
    <html>
        <head>
            <title>Documentación de la API</title>
            <style>
            body {
                font-family: Arial, Helvetica, sans-serif;
			}
            </style>
        </head>
        <body>
            <h1>Documentación de la API</h1>
            <p>Esta es la documentación para la API proporcionada.</p>
        
            <h2>Endpoints</h2>
        
            <h3>/StorePlayTimeGenre [GET]</h3>
            <p>Descripción: Almacena datos de tiempo de juego por género en la base de datos.</p>
        
            <h3>/StoreSteamGames [GET]</h3>
            <p>Descripción: Almacena datos de juegos de Steam en la base de datos.</p>
        
            <!-- Agrega documentación para otros endpoints -->
        
            <!-- Ejemplo de endpoints con parámetros -->
            <h3>/PlayTimeGenre/{genre} [GET]</h3>
            <p>Descripción: Obtiene el año de lanzamiento con más horas jugadas para un género específico.</p>
            <p>Parámetros:</p>
            <ul>
                <li>genre (string): El género para el que se desea obtener información.</li>
            </ul>
        
            <h3>/UserForGenre/{genre} [GET]</h3>
            <p>Descripción: Obtiene al usuario con más horas jugadas para un género específico.</p>
            <p>Parámetros:</p>
            <ul>
                <li>genre (string): El género para el que se desea obtener información.</li>
            </ul>
        
            <h3>/UsersRecommend/{year} [GET]</h3>
            <p>Descripción: Obtiene los 3 juegos más recomendados para un año específico.</p>
            <p>Parámetros:</p>
            <ul>
                <li>year (string): El año del cual se desean obtener juegos recomendados.</li>
            </ul>
        </body>
    </html>
    """
    return html


@app.get("/StorePlayTimeGenre")
async def StorePlayTimeGenre():
    # Conexión a la base de datos
    conn = sqlite3.connect('data_sources/steam.db')
    cursor = conn.cursor()

    # Directorio que contiene los archivos Parquet
    dir = 'data_sources/parquet/play_time_genre'

    # Recorrer los archivos Parquet en el directorio
    for file in os.listdir(dir):
        if file.endswith('.parquet'):
            # Leer el archivo Parquet
            df = pq.read_table(os.path.join(dir, file)).to_pandas()

            # Almacenar los datos en la base de datos SQLite
            df.to_sql('play_time_genre', conn, if_exists='append', index=False)

    # Confirmar y cerrar la conexión a la base de datos SQLite
    conn.commit()

    # Crear un índice en una tabla
    cursor.execute('CREATE INDEX genres_index ON play_time_genre (genres);')

    # Cerrar la conexión
    conn.close()


@app.get("/StoreSteamGames")
async def StoreSteamGames():
    # Conexión a la base de datos
    conn = sqlite3.connect('data_sources/steam.db')

    # Directorio que contiene los archivos Parquet
    dir = 'data_sources/parquet/steam_games'

    # Recorrer los archivos Parquet en el directorio
    for file in os.listdir(dir):
        if file.endswith('.parquet'):
            # Leer el archivo Parquet
            df = pq.read_table(os.path.join(dir, file)).to_pandas()
            df['genres'] = df['genres'].astype(str)

            # Almacenar los datos en la base de datos SQLite
            df.to_sql('steam_games', conn, if_exists='append', index=False)

    # Confirmar y cerrar la conexión a la base de datos SQLite
    conn.commit()

    # Cerrar la conexión
    conn.close()


@app.get("/StoreUserItems")
async def StoreUserItems():
    # Conexión a la base de datos
    conn = sqlite3.connect('data_sources/steam.db')

    # Directorio que contiene los archivos Parquet
    dir = 'data_sources/parquet/user_items'

    # Recorrer los archivos Parquet en el directorio
    for file in os.listdir(dir):
        if file.endswith('.parquet'):
            # Leer el archivo Parquet
            df = pq.read_table(os.path.join(dir, file)).to_pandas()

            # Almacenar los datos en la base de datos SQLite
            df.to_sql('user_items', conn, if_exists='append', index=False)

    # Confirmar y cerrar la conexión a la base de datos SQLite
    conn.commit()

    # Cerrar la conexión
    conn.close()


@app.get("/StoreUserReviews")
async def StoreUserReviews():
    # Conexión a la base de datos
    conn = sqlite3.connect('data_sources/steam.db')

    # Directorio que contiene los archivos Parquet
    dir = 'data_sources/parquet/user_reviews'

    counter = 1

    # Recorrer los archivos Parquet en el directorio
    for file in os.listdir(dir):
        if file.endswith('.parquet'):
            if counter <= 100:
                # Leer el archivo Parquet
                df = pq.read_table(os.path.join(dir, file)).to_pandas()

                # Almacenar los datos en la base de datos SQLite
                df.to_sql('user_reviews', conn,
                          if_exists='append', index=False)
            counter += 1

    # Confirmar y cerrar la conexión a la base de datos SQLite
    conn.commit()

    # Cerrar la conexión
    conn.close()


@app.get("/PlayTimeGenre/{genre}")
async def PlayTimeGenre(genre: str):
    # Conectar a la base de datos SQLite
    conn = sqlite3.connect('data_sources/steam.db')
    cursor = conn.cursor()

    # Ejecutar la consulta SQL
    query = f"""
        SELECT release_year, SUM(playtime_forever) AS total_playtime
        FROM play_time_genre
        WHERE genres LIKE '%{genre}%'
        GROUP BY release_year
        ORDER BY total_playtime DESC
        LIMIT 1;
    """

    # Ejecutar la consulta y obtener resultados
    cursor.execute(query)
    result = cursor.fetchone()

    # Cerrar la conexión
    conn.close()

    if result:
        release_year = result[0]
        tota_played_hours = result[1]
        return {
            f"El año de lanzamiento con más horas jugadas para el género '{genre}' es {release_year} con un total de {tota_played_hours} horas jugadas."}
    else:
        return {
            f"No se encontraron datos para el género '{genre}' en la base de datos."}


@app.get("/UserForGenre/{genre}")
async def UserForGenre(genre: str):
    # Encontrar el usuario con más horas jugadas para el género dado relacionando las tablas steam_games y user_items
    # Conectar a la base de datos SQLite
    conn = sqlite3.connect('data_sources/steam.db')
    cursor = conn.cursor()

    # Ejecutar la consulta SQL
    query = f"""
		SELECT user_id, SUM(playtime_forever) AS total_playtime
		FROM user_items
		WHERE item_id IN (
			SELECT id
			FROM steam_games
			WHERE genres LIKE '%{genre}%'
		)
		GROUP BY user_id
		ORDER BY total_playtime DESC
		LIMIT 1;
	"""

    # Ejecutar la consulta y obtener resultados
    cursor.execute(query)
    result = cursor.fetchone()

    # Cerrar la conexión
    conn.close()

    if result:
        user_id = result[0]
        tota_played_hours = result[1]
        return {
            f"El usuario con más horas jugadas para el género '{genre}' es {user_id} con un total de {tota_played_hours} horas jugadas."}
    else:
        return {
            f"No se encontraron datos para el género '{genre}' en la base de datos."}


@app.get("/UsersRecommend/{year}")
async def UsersRecommend(year: int):
    # Conectar a la base de datos SQLite
    conn = sqlite3.connect('data_sources/steam.db')
    cursor = conn.cursor()

    # Ejecutar la consulta SQL
    query = f"""
		SELECT steam_games.title
		FROM user_reviews
		INNER JOIN steam_games ON user_reviews.item_id = steam_games.id
		WHERE user_reviews.recommend = true AND user_reviews.review > 0 AND user_reviews.posted LIKE '%{year}%'
		GROUP BY user_reviews.item_id
		ORDER BY COUNT(user_reviews.item_id) DESC
		LIMIT 3;
	"""

    # Ejecutar la consulta y obtener resultados
    cursor.execute(query)
    result = cursor.fetchall()

    # Cerrar la conexión
    conn.close()

    if result:
        return {
            f"Los 3 juegos más recomendados para el año {year} son: {result[0][0]}, {result[1][0]} y {result[2][0]}."}
    else:
        return {
            f"No se encontraron datos para el año {year} en la base de datos."}


@app.get("/UsersNotRecommend/{year}")
async def UsersNotRecommend(year: int):
    # Conectar a la base de datos SQLite
    conn = sqlite3.connect('data_sources/steam.db')
    cursor = conn.cursor()

    # Ejecutar la consulta SQL
    query = f"""
		SELECT steam_games.title
		FROM user_reviews
		INNER JOIN steam_games ON user_reviews.item_id = steam_games.id
		WHERE user_reviews.recommend = false AND user_reviews.review = 0 AND user_reviews.posted LIKE '%{year}%'
		GROUP BY user_reviews.item_id
		ORDER BY COUNT(user_reviews.item_id) DESC
		LIMIT 3;
	"""

    # Ejecutar la consulta y obtener resultados
    cursor.execute(query)
    result = cursor.fetchall()

    # Cerrar la conexión
    conn.close()

    if result:
        return {
            f"Los 3 juegos menos recomendados para el año {year} son: {result[0][0]}, {result[1][0]} y {result[2][0]}."}
    else:
        return {
            f"No se encontraron datos para el año {year} en la base de datos."}
