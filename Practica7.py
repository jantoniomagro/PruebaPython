# librerías necesarias
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import re
import os
import threading

# variable para limitar el número de líneas que se leen del fichero
numMaxLineas = 100

# configuración de Kafka
bootstrap_servers = 'localhost:9092'

# topics
topic_delete = 'topic_delete'
topic_post = 'topic_post'
topic_get = 'topic_get'
topic_put = 'topic_put'

# Inicializar productor de Kafka
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# Función para enviar un mensaje al topic especificado
def enviar_mensaje(topic, mensaje):
    try:
        producer.send(topic, mensaje.encode('utf-8')).get()
        print(f"Mensaje enviado a {topic}: {mensaje}")
    except KafkaError as e:
        print(f"Error al enviar el mensaje a {topic}: {str(e)}")

# Función para leer el archivo de log y enviar las líneas a los topics correspondientes
def procesar_fichero_log(ruta_fichero):
    contador = 0
    with open(ruta_fichero, 'r') as log_file:
        for line in log_file:
            if contador < numMaxLineas: # esto para no tener que leer las miles de líneas del fichero
                contador+=1

                # partimos la línea del fichero para quedarnos solo con los nombres de los métodos
                patron = r'(\S+) (\S+) (\S+) \[(.*?)\] "(.*?)" (\d+) (\d+) "(.*?)" "(.*?)" (\d+)'
                resultado = re.match(patron, line)

                if resultado:
                    metodo = resultado.group(5)
                    nombre_metodo = metodo.split()[0]

                # Obtener el tipo de operación de la línea
                operation = line.split(' ')[5].replace('"', '')
                message = line.strip()

                # Enviar la línea al topic correspondiente según el tipo de operación
                if operation == 'DELETE':
                    enviar_mensaje(topic_delete, message)
                elif operation == 'POST':
                    enviar_mensaje(topic_post, message)
                elif operation == 'GET':
                    enviar_mensaje(topic_get, message)
                elif operation == 'PUT':
                    enviar_mensaje(topic_put, message)
                else:
                    print(f"Tipo de operación desconocido: {operation}")

# Inicializar consumidores Kafka para cada topic
consumer_delete = KafkaConsumer(topic_delete, bootstrap_servers=bootstrap_servers, group_id='consumer_group_delete')
consumer_post = KafkaConsumer(topic_post, bootstrap_servers=bootstrap_servers, group_id='consumer_group_post')
consumer_get = KafkaConsumer(topic_get, bootstrap_servers=bootstrap_servers, group_id='consumer_group_get')
consumer_put = KafkaConsumer(topic_put, bootstrap_servers=bootstrap_servers, group_id='consumer_group_put')

# Función para imprimir los mensajes recibidos por cada consumidor
def consumir_mensajes(consumer, topic):
    print(f"Consumiendo mensajes del topic {topic}")
    for message in consumer:
        print(f"Mensaje recibido de {topic}: {message.value.decode('utf-8')}")

# Ejecución del programa, el fichero lo lee del directorio actual
if __name__ == '__main__':
    ruta_actual = os.getcwd()
    nombre_fichero = "logfiles.log"
    ruta_completa = ruta_actual + "/" + nombre_fichero
    procesar_fichero_log(ruta_completa)

    # Leer mensajes para cada consumidor en hilos
    t1 = threading.Thread(target=consumir_mensajes, args=(consumer_delete, topic_delete))
    t2 = threading.Thread(target=consumir_mensajes, args=(consumer_post, topic_post))
    t3 = threading.Thread(target=consumir_mensajes, args=(consumer_get, topic_get))
    t4 = threading.Thread(target=consumir_mensajes, args=(consumer_put, topic_put))

    t1.start()
    t2.start()
    t3.start()
    t4.start()
