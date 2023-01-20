#importamos las librerias
import pandas as pd                                     #Manejo de Dataframes
from fastapi import FastAPI                             #Implementación de la API
from fastapi.responses import PlainTextResponse         #Output de la información en formato texto
from pandasql import sqldf                              #Consultas sobre dataframes mediante lenguaje SQL

#importamos el dataframe anteriormente generado y en base a eso creamos las funciones
df_plataforma = pd.read_csv ("plataformas_database.csv")

app = FastAPI()

pysqldf = lambda q: sqldf(q, globals())

#Consigna Nº1: Cantidad de veces que aparece una keyword en el título de peliculas/series, por plataforma

@app.get(
    "/get_word_count/{plataforma}/{keyword}",
    response_class=PlainTextResponse,
    tags=["Contar palabras"],
)
def get_word_count(keyword: str, plataforma: str):
    """
   Esta función cuenta la cantidad de títulos de acuerdo a la keyword asignada (por ejemplo: the)
   Para ello se necesita ingresar la keyword y una plataforma (amazon, disney, hulu, amazon), 
   se requiere ingresar los datos en minuscula

    """
    if plataforma == "amazon":
        plat = "a%"
    elif plataforma == "netflix":
        plat = "n%"
    elif plataforma == "hulu":
        plat = "h%"
    elif plataforma == "disney":
        plat = "d%"
    else:
        return "No ha seleccionado la plataforma entre las opciones posibles"

    query = (
        """SELECT COUNT(title)
        FROM df_plataforma
        WHERE title LIKE '%"""
        + keyword
        + """%'
        AND id LIKE '"""
        + plat
        + """' """
    )
    veces = pysqldf(query)
    return veces.to_string(index=False, header=False)

#Consigna Nº2 Cantidad de películas por plataforma con un puntaje mayor a XX en determinado año

@app.get(
    "/get_score_count/{plataforma}/{score}/{anio}",
    response_class=PlainTextResponse,
    tags=["Cantidad puntaje mínimo"],
)
def get_score_count(plataforma: str, score: str, anio: str):
    """
    Cuenta cantidad de **películas** que superan el score indicado para cierto año y plataforma.
    Requiere el ingreso del **año** en formato AAAA, el **score** como un entero del 0 a 99
    y la **plataforma** (netflix, disney, hulu, amazon)
    """
    if plataforma == "amazon":
        plat = "a%"
    elif plataforma == "netflix":
        plat = "n%"
    elif plataforma == "hulu":
        plat = "h%"
    elif plataforma == "disney":
        plat = "d%"
    else:
        return "No ha seleccionado la plataforma entre las opciones posibles"

    query = (
        """SELECT COUNT(title)
        FROM df_plataforma
        WHERE score > 20 """
        + score
        + """
        AND release_year = """
        + anio
        + """
        AND id LIKE '"""
        + plat
        + """'
        AND type = "movie" """
    )
    cantidad = pysqldf(query)
    return cantidad.to_string(index=False, header=False)

#Consigna Nº3 La segunda película con mayor score para una plataforma determinada, según el orden alfabético de los títulos.

@app.get(
    "/get_second_score/{plataforma}",
    response_class=PlainTextResponse,
    tags=["Segunda mayor score"],
)
def get_second_score(plataforma: str):
    """
    Esta función devuelve la segunda película con mayor score de una plataforma seleccionada 
    Para ellos requiere ingresar la plataforma (amazon, disney, hulu, netflix)
    en minúsculas.
    """
    if plataforma == "amazon":
        plat = "a%"
    elif plataforma == "netflix":
        plat = "n%"
    elif plataforma == "hulu":
        plat = "h%"
    elif plataforma == "disney":
        plat = "d%"
    else:
        return "No ha seleccionado la plataforma entre las opciones posibles"

    query = (
        """SELECT title
        FROM df_plataforma
        WHERE id LIKE '"""
        + plat
        + """'
        AND type = "movie"
        ORDER BY score DESC, title ASC
        LIMIT 1,1"""
    )
    segunda = pysqldf(query)
    return segunda.to_string(index=False, header=False)

#Consigna Nº4 Película que más duró según año, plataforma y tipo de duración

@app.get(
    "/get_longest/{plataforma}/{tipo_duracion}/{anio}",
    response_class=PlainTextResponse,
    tags=["Mayor duración"],
)
def get_longest(plataforma: str, tipo_duracion: str, anio: str):
    """
    Esta función calcula cual es la película con mayor duración en minutos o temporadas para ello 
    se requiere ingresar el “anio” en el formato AAAA (por ejemplo 2010), 
    el tipo de duración que desean saber (min/season) 
    y la plataforma que deseen (amazon, disney, hulu, netflix), todo esto debe ir en minuscula
    """
    if plataforma == "amazon":
        plat = "a%"
    elif plataforma == "netflix":
        plat = "n%"
    elif plataforma == "hulu":
        plat = "h%"
    elif plataforma == "disney":
        plat = "d%"
    else:
        return "No ha seleccionado la plataforma entre las opciones posibles"

    query = (
        """SELECT title
        FROM df_plataforma
        WHERE id LIKE '"""
        + plat
        + """'
        AND release_year = '"""
        + anio
        + """'
        AND duration_type = '"""
        + tipo_duracion
        + """'
        AND duration_int = (SELECT MAX(duration_int)
        FROM df_plataforma
        WHERE id LIKE '"""
        + plat
        + """'
        AND release_year = '"""
        + anio
        + """'
        AND duration_type = '"""
        + tipo_duracion
        + """')
        """
    )

    mayorduracion = pysqldf(query)
    return mayorduracion.to_string(index=False, header=False)

#Consigna Nº5 Cantidad de series y películas por rating

@app.get(
    "/get_rating_count/{rating}",
    response_class=PlainTextResponse,
    tags=["Cantidad por rating"],
)
def get_rating_count(rating: str):
    """
    Esta función devuelve el total de películas y series de acuerdo a su rating ingresando los 
    siguientes valores:
    (16, 13+, 16+, 18+, 7+, ages_16_, ages_18_, all, all_ages, g, nc-17,
    not rated, not_rate, nr, pg, pg-13, r, tv-14, tv-g, tv-ma, tv-nr, tv-pg, tv-y,
    tv-y7, tv-y7-fv, unrated, ur)

    """
    query = (
        """SELECT count(title)
        FROM df_plataforma
        WHERE rating = '"""
        + rating
        + """' """
    )
    total = pysqldf(query)
    return total.to_string(index=False, header=False)

    