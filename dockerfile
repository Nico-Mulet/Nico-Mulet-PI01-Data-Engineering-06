#Utilizamos la imgen de tiangolo
FROM tiangolo/uvicorn-gunicorn-fastapi

#instalamos pandas
RUN pip install pandas
RUN pip install pandasql

#abrimos el puerto que utilizaremos
EXPOSE 80

#copia todo el contenido de la carpeta "./app" hacia el contenedor con la dirección "/app"
COPY ./app /app





