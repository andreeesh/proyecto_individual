from fastapi import FastAPI
import pandas as pd

app = FastAPI()


@app.get("/PlayTimeGenre/{genre}")
def PlayTimeGenre(genre: str):
    chunk_size = 10000  # Tamaño de chunk para lectura de parquet

    # Utiliza chunks para cargar los datos en bloques
    steam_games_reader = pd.read_parquet(
        "data_sources/parquet/steam_games.parquet", chunksize=chunk_size)
    users_items_reader = pd.read_parquet(
        "data_sources/parquet/users_items.parquet", chunksize=chunk_size)

    merged_data = pd.DataFrame()
    for chunk_steam_games in steam_games_reader:
        for chunk_users_items in users_items_reader:
            chunk_steam_games['id'] = chunk_steam_games['id'].astype(int)
            chunk_steam_games['genres'] = chunk_steam_games['genres'].astype(
                str)
            chunk_users_items['item_id'] = chunk_users_items['item_id'].astype(
                int)

            merged_chunk = pd.merge(
                chunk_steam_games, chunk_users_items, left_on='id', right_on='item_id')
            filtered_chunk = merged_chunk[merged_chunk['genres'].str.contains(
                genre)]

            if not filtered_chunk.empty:
                grouped_chunk = filtered_chunk.groupby(
                    'release_year')['playtime_forever'].sum()
                max_playtime_year = grouped_chunk.idxmax()
                del grouped_chunk
                return {f"El año de lanzamiento con la mayor cantidad de horas jugadas para el género {genre} es el {max_playtime_year}"}
            else:
                return {f"No se encontraron registros para el género {genre}"}

    return {f"No se encontraron registros para el género {genre}"}


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
