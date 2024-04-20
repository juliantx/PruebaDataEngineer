import pandas as pd
import mysql.connector
import os
from datetime import datetime


#Cargamos la ruta de la carpeta actual
ruta_carpeta_actual = os.path.abspath(os.path.dirname(__file__))
"""Cargamos un grupo de parametros guardados, esto es
para poder reutilizar el script con otras archivos por lotes que tengan otras variables
y poder cambiar tambien las consultas y las credenciales de base de datos
el CSV que cargaremos tiene estos valores predefinidos:

nombreBaseLotes,userDB,password,host,database,query
"2012-","admin","admin","localhost","prueba","query_de_la_base_de_datos"
"""

ruta_archivo_parametros = ruta_carpeta_actual +"/parametros.csv"
parametros = pd.read_csv(ruta_archivo_parametros)




""" Recorremos el directorio para validar archivos de micro-batch """
lita_de_archivos = os.listdir(ruta_carpeta_actual+"/archivos/")
# Contador para el número de archivos con el prefijo dado
total_archivos_lote = 0
# Recorremos los archivos en el directorio
for archivo in lita_de_archivos:
# Verificar si el archivo comienza con el nombreBaseArchivo
    if archivo.startswith(parametros["nombreBaseArchivo"][0]):
        total_archivos_lote += 1

lote = 0
while(lote < total_archivos_lote):
#for i in range (lote, total_archivos_lote):

    """
    Cargamos un archivo que se ira actualizando por cada lote que se envie y sus metricas
    |Lote_enviado|Timestamp|Max|Min|Promedio|Filas_cargadas|
    """

    archivo_lotes_ruta = ruta_carpeta_actual + "/lotes.csv"
    lotes = pd.read_csv(archivo_lotes_ruta)
    lote = lotes["Lote_enviado"][0] + 1 # variable para cargar el lote actual





    """
    A continuación crearemos un ciclo para recorrer los archivos
    del micro-batch que no ha sido enviados, esto e hace por si
    se anexan archivos futuros al micro-batch, es decir archivos
    como 2021-6, 2021-7. Siempre el ciclo iniciará desde el valor
    de lote guardado + 1, es decir si guardamos ya los 5 primeros lotes
    iniciara desde 6 para detectar los nuevos archivos del lote
    """



    ########### Cargamos el/los archivos ########

    # Verificamos si el archivo es por lotes si no puede ser un archivo simple como validation.csv

    if parametros["porLotes"][0] == "si":

        #El nombre base del archivo por lotes lo tenemos guardado en el dataframe de parametros 
        archivo = ruta_carpeta_actual +"/archivos/" + parametros["nombreBaseArchivo"][0] + str(lote) +".csv"
    else:
        archivo = ruta_carpeta_actual +"/archivos/" + parametros["nombreBaseArchivo"][0] +".csv"
    #Validamos si el archivo por lotes existe sino puede que sea otro archivo con otra configuracion
    if os.path.exists(archivo):
        precios = pd.read_csv(archivo)

        print("\n\nArchivo procesado:", parametros["nombreBaseArchivo"][0] +".csv\n")

    ############## Transformacion ######################

        # Limpieza de valores invalidos Null Nan
        precios = precios.dropna().reset_index(drop=True)
        # Corrección formato timestamp
        precios['timestamp'] = pd.to_datetime(precios['timestamp'], format='%m/%d/%Y')

        #Guardamos el nombre de las columnas
        columns = precios.columns.to_list()


    ################## Carga ######################################

        filas_cargadas = lotes["Filas_cargadas"][0] #almacenamos solo las filas que se han cargado a la base de datos
        max_price = lotes["Max"][0] # almacenamos el máximo actual
        min_price = lotes["Min"][0] # almacenamos el minimo actual
        promedio = lotes["Promedio"][0] #almacenamos el promedio
        acumulado = lotes["AcumuladoPrice"][0] #Precio acumulado

        # Define la configuración de conexión a la base de datos MySQL
        config = {
            "user": parametros["userDB"][0],
            "password": parametros["password"][0],
            "host": parametros["host"][0],
            "database": parametros["database"][0]
        }

        # Establece una conexión a la base de datos MySQL
        connection = mysql.connector.connect(**config)

        # Crea un cursor para ejecutar consultas SQL
        cursor = connection.cursor()

        # Vamos a guardar de una fila en la base de datos por cada lote que se procesa
        for index, row in precios.iterrows():

            #Creamos una tupla con las columnas del dataframe para los valores de la consulta
            valores = (row[columns[0]],)
            for j in range(1,len(columns)):
                valores += (row[columns[j]],) #Construcción de la tupla

            try:
                # Cargamos el query predefinido en una archivo aparte para poder reusar script con otras tablas
                query = parametros["query"][0]
                values = valores
                cursor.execute(query, values)
                # Confirma los cambios en la base de datos
                connection.commit()

                     
                filas_cargadas+=1
                #Nos aseguramos que el precio máximo y minimo solo se aplique para los registros cargados
                #Por eso vamos hacer una comparación fila a fila y no con el dataframe
                if row["price"] > max_price:
                    max_price = row["price"]

                if row["price"] < min_price:
                    min_price = row["price"]

                acumulado += row["price"]
                promedio = acumulado/(filas_cargadas)

                print("Filas cargadas:",filas_cargadas,
                      "precio_enviado: ", row["price"],
                      "Max:",max_price,
                      "Min:",min_price,
                      "Prom:",promedio)
            except mysql.connector.Error as err:
                # En caso de error, imprime la fila del proble, el mensaje de error y deshace los cambios
                print(f"Error al insertar la fila {index + 1}: {err}")
                connection.rollback()
        # Cierra el cursor y la conexión
        cursor.close()
        connection.close()

        print("máximo:", max_price)
        print("minimo:", min_price)
        print("Promedio:", promedio)
        print("Filas cargadas:",filas_cargadas)

    #### Creamos una lista con los valores de las metricas y el lote terminado
        # Obtener la fecha y hora actual
        fecha_actual = datetime.now()
        # Formatear la fecha en el formato deseado
        fecha_formateada = fecha_actual.strftime("%d/%m/%Y")
        terminado = [lote, fecha_formateada,max_price,min_price,acumulado,promedio,filas_cargadas]
        #Remplazamos en el dataframe lotes y lo guardamos
        lotes.iloc[0] = terminado
        lotes.to_csv(archivo_lotes_ruta,index=False)
    else:
        print("No hay archivos pendientes de procesar")
        #print("1")

