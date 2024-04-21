from kafka import KafkaConsumer,TopicPartition
import json
import os
import pandas as pd
import mysql.connector
import time

#Cargamos la ruta de la carpeta donde esta el script y los archivos
ruta_carpeta_actual = os.path.abspath(os.path.dirname(__file__))
ruta_archivo_parametros = ruta_carpeta_actual +"/parametros.csv"
parametros = pd.read_csv(ruta_archivo_parametros)

#Configuración de Kafka
bootstrap_servers = 'localhost:9092'
topic = 'pragmaPrueba'
partition = 0  # Número de partición de la que se desea consumir

#Configuración del consumidor de Kafka
consumer = KafkaConsumer(
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',
    group_id="cliente_pragma",
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

#Se configurar el consumer
consumer.assign([TopicPartition(topic, partition)]) 
config = {
    "user": parametros["userDB"][0],
    "password": parametros["password"][0],
    "host": parametros["host"][0],
    "database": parametros["database"][0]
}
#Variable de conexión a base de datos MySQL
connection = mysql.connector.connect(**config)
#Cursor para ejecución de consultas
cursor = connection.cursor()

#Llevar el registro de datos procesados
contador_filas = 0

#Iterar para consumir mensajes
for message in consumer:
    values = tuple(message.value)
    print(values)

    #Validamos cantidad de filas almacena en la clave
    if message.key==b'filasEnviadas':
        print("Clave: ",message.key)
        print(message.value)
        if int(message.value) == contador_filas:
            break
    contador_filas+=1
    try:
    #Cargamos el query predefinido en parametros y hacemos inserción
        query = parametros["query"][0]
        cursor.execute(query, values)
        connection.commit()
    except mysql.connector.Error as err:
        print(f"Error")
        connection.rollback()
consumer.close()
