from fastapi import FastAPI
import pandas as pd
import sqlite3
import pyarrow.parquet as pq
import os

app = FastAPI()


@app.get("/EtlPlayTimeGenre")
async def EtlPlayTimeGenre():
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
def UserForGenre(genre: str):
    df_steam_games = pd.read_parquet(
        "data_sources/parquet/steam_games.parquet")
    df_users_items = pd.read_parquet(
        "data_sources/parquet/users_items.parquet")

    # Convierte el tipo de dato para poder hacer el merge
    df_steam_games['id'] = df_steam_games['id'].astype(int)
    df_steam_games['genres'] = df_steam_games['genres'].astype(str)
    df_users_items['item_id'] = df_users_items['item_id'].astype(int)

    merged_df = pd.merge(df_steam_games, df_users_items,
                         left_on='id', right_on='item_id')

    filtered_df = merged_df[merged_df['genres'].str.contains(genre)]

    if not filtered_df.empty:
        # Encontrar el usuario con más horas jugadas para el género dado
        max_playtime_user = filtered_df.groupby(
            'user_id')['playtime_forever'].sum().idxmax()

        # Filtro el dataframe por el usuario con mas horas jugadas
        user_filtered_df = filtered_df[filtered_df['user_id']
                                       == max_playtime_user]

        # Calcular la acumulación de horas jugadas por año
        hours_by_year = user_filtered_df.groupby(
            'release_year')['playtime_forever'].sum().reset_index()
        hours_by_year = hours_by_year.rename(
            columns={'release_year': 'Año', 'playtime_forever': 'Horas'})
        hours_list = hours_by_year.to_dict(orient='records')

        # Crear el diccionario de retorno
        result = {
            "Usuario con más horas jugadas para " + genre: max_playtime_user,
            "Horas jugadas": hours_list
        }
    else:
        result = {f"No se encontraron registros para el género {genre}"}
    return result


@app.get("/UsersRecommend/{year}")
def UsersRecommend(year: str):
    df_steam_games = pd.read_parquet(
        "data_sources/parquet/steam_games.parquet")
    df_users_reviews = pd.read_parquet(
        "data_sources/parquet/users_reviews.parquet")

    df_steam_games['id'] = df_steam_games['id'].astype(int)
    df_users_reviews['item_id'] = df_users_reviews['item_id'].astype(int)

    merged_df = pd.merge(df_steam_games, df_users_reviews,
                         left_on='id', right_on='item_id')

    df_filtered_by_year = merged_df[merged_df["posted"].str.contains(year)]

    if not df_filtered_by_year.empty:
        df_filtered_by_recommend = df_filtered_by_year[df_filtered_by_year["recommend"] == True]
        df_filtered_by_review = df_filtered_by_recommend[df_filtered_by_recommend["review"] > 0]

        # Muestra los 3 juegos mas recomendados
        result = df_filtered_by_review.groupby(
            ['id']).size().sort_values(ascending=False).head(3)
        first_place = df_steam_games[df_steam_games['id']
                                     == result.index[0]]['title'].values[0]
        second_place = df_steam_games[df_steam_games['id']
                                      == result.index[1]]['title'].values[0]
        third_place = df_steam_games[df_steam_games['id']
                                     == result.index[2]]['title'].values[0]
        return [{"Puesto 1: ": first_place}, {"Puesto 2: ": second_place}, {"Puesto 3: ": third_place}]
    else:
        return {f"No se encontraron registros para el año {year}"}
