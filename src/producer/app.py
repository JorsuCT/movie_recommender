import boto3
import json
import time
import random
import os
from dotenv import load_dotenv

load_dotenv()

SQS_URL = os.getenv('MOVIE-QUEUE-RAW_URL')

if not SQS_URL:
    raise Exception("ERROR: No se encuentra MOVIE-QUEUE-RAW_URL en el archivo .env")

#sqs = boto3.client('sqs', region_name='us-east-1')
sqs = boto3.client(
        'sqs',
        region_name='us-east-1',
        endpoint_url='http://localhost:4566',  # <--- ¡ESTA LÍNEA ES VITAL!
        aws_access_key_id='test',              # Necesario para LocalStack
        aws_secret_access_key='test'           # Necesario para LocalStack
    )

def generar_datos_prueba():
    print(f"--- SIMULADOR DE USUARIOS ---")
    print(f"Enviando a: {SQS_URL}\n")

    movie_ids_validos = [1, 2, 3, 5, 6]

    while True:
        evento = {
            'userId': random.randint(1, 500),
            'movieId': random.choice(movie_ids_validos),
            'rating': round(random.uniform(0.5, 5.0), 1),
            'timestamp': time.time()
        }

        try:
            sqs.send_message(
                QueueUrl=SQS_URL,
                MessageBody=json.dumps(evento)
            )
            print(f"Enviado: User {evento['userId']} -> Movie {evento['movieId']} ({evento['rating']}*)")
        except Exception as e:
            print(f"Error enviando: {e}")

        time.sleep(2)

if __name__ == "__main__":
    generar_datos_prueba()
