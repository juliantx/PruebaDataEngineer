import pandas as pd
import mysql.connector
import os
from datetime import datetime
from kafka import KafkaProducer
import json

class KafkaMessageSender:
    def __init__(self, topic_name,partition_number, bootstrap_servers='localhost:9092'):
        self.topic_name = topic_name
        self.partition_number = partition_number
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    def send_message_to_partition(self, message,key_value):
        #Codifica el mensaje en formato JSON
        message_json = json.dumps(message).encode('utf-8')
        #Envía el mensaje a la partición específica
        self.producer.send(self.topic_name, message_json,key_value,partition=self.partition_number)
        #Espera a que todos los mensajes sean enviados antes de cerrar el productor
        self.producer.flush()

    def close(self):
        #Cierra el productor de Kafka
        self.producer.close()

class ProcesadorMicroBatch:

    def __init__(self, ruta_carpeta_actual,parametros):
        self.ruta_carpeta_actual = ruta_carpeta_actual
        self.parametros = parametros
        
    def contar_archivos_por_lote(self):
        total_archivos_lote = 0
        lita_de_archivos = os.listdir(ruta_carpeta_actual+"/archivos/")
        for archivo in lita_de_archivos:
            if archivo.startswith(self.parametros ["nombreBaseArchivo"][0]):
                total_archivos_lote += 1
        return total_archivos_lote

    def cargar_archivo_lotes(self):
        # Cargamos el archivo de lotes
        archivo_lotes_ruta = self.ruta_carpeta_actual + "/lotes.csv"
        lotes = pd.read_csv(archivo_lotes_ruta)
        return lotes, archivo_lotes_ruta


    def limpiar_precios(self,precios):
        # Limpieza de valores inválidos NaN
        precios = precios.dropna().reset_index(drop=True)
        # Corrección formato timestamp
        precios['timestamp'] = pd.to_datetime(precios['timestamp'], format='%m/%d/%Y')
        return precios

    def procesar_archivos(self, topic_name, partition_number):
        lote = 0
        total_archivos_lote = self.contar_archivos_por_lote()
        while lote < total_archivos_lote:
            #archivo_lotes_ruta = os.path.join(self.ruta_carpeta_actual, "lotes.csv")
            lotes, archivo_lotes_ruta = self.cargar_archivo_lotes()
            lote = lotes["Lote_enviado"][0] + 1  # variable para cargar el lote actual

            if parametros["porLotes"][0] == "si":
                archivo = self.ruta_carpeta_actual +"/archivos/" + self.parametros["nombreBaseArchivo"][0] + str(lote) +".csv"
                print("\n\nArchivo procesado:", self.parametros["nombreBaseArchivo"][0] + str(lote) + ".csv\n")
            else:
                archivo = self.ruta_carpeta_actual +"/archivos/" + self.parametros["nombreBaseArchivo"][0] +".csv"
        
            if os.path.exists(archivo):
                precios = self.limpiar_precios(pd.read_csv(archivo))
               

                filas_cargadas = lotes["Filas_cargadas"][0]
                max_price = lotes["Max"][0]
                min_price = lotes["Min"][0]
                promedio = lotes["Promedio"][0]
                acumulado = lotes["AcumuladoPrice"][0]

                columns = precios.columns.to_list()

                kafka_sender = KafkaMessageSender(topic_name, partition_number)

                for index, row in precios.iterrows():
                    timestamp_str = row[columns[0]].strftime('%Y-%m-%d %H:%M:%S')
                    datos = (timestamp_str,)
                    for j in range(1, len(columns)):
                        datos += (row[columns[j]],)
                    try:
                        kafka_sender.send_message_to_partition(datos,None)

                        filas_cargadas += 1
                        if row["price"] > max_price:
                            max_price = row["price"]

                        if row["price"] < min_price:
                            min_price = row["price"]

                        acumulado += row["price"]
                        promedio = acumulado / (filas_cargadas)

                        print("Filas cargadas:", filas_cargadas,
                              "precio_enviado: ", row["price"],
                              "Max:", max_price,
                              "Min:", min_price,
                              "Prom:", promedio)
                    except Exception as err:
                        print(f"Error al insertar la fila")

                print("Máximo:", max_price)
                print("Mínimo:", min_price)
                print("Promedio:", promedio)
                print("Filas cargadas:", filas_cargadas)

                fecha_actual = datetime.now()
                fecha_formateada = fecha_actual.strftime("%d/%m/%Y")
                terminado = [lote, fecha_formateada, max_price, min_price, acumulado, promedio, filas_cargadas]
                lotes.iloc[0] = terminado
                lotes.to_csv(archivo_lotes_ruta, index=False)
                if lote == total_archivos_lote:
                    kafka_sender.send_message_to_partition(str(filas_cargadas),b'filasEnviadas')

                kafka_sender.close()
            
            else:

                print("No hay archivos pendientes de procesar")



if __name__ == "__main__":
    ruta_carpeta_actual = os.path.abspath(os.path.dirname(__file__))
    parametros = pd.read_csv(os.path.join(ruta_carpeta_actual, "parametros.csv"))
    topic_name = 'pragmaPrueba'
    partition = 0

    batch_processor = ProcesadorMicroBatch(ruta_carpeta_actual, parametros)
    batch_processor.procesar_archivos(topic_name, partition)
