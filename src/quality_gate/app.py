import boto3
import json
import os
import sys
import datetime
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.common.schemas import EventoValoracion
from pydantic import ValidationError

load_dotenv()

RAW_URL = os.getenv('MOVIE-QUEUE-RAW_URL')
CLEAN_URL = os.getenv('MOVIE-QUEUE-CLEAN_URL')
DLQ_URL = os.getenv('MOVIE-QUEUE-DLQ_URL')
BUCKET_NAME = os.getenv('DATALAKE_BUCKET', 'movie-datalake')

#sqs = boto3.client('sqs', region_name='us-east-1')
sqs = boto3.client(
        'sqs',
        region_name='us-east-1',
        endpoint_url='http://localhost:4566',  # <--- ¡ESTA LÍNEA ES VITAL!
        aws_access_key_id='test',              # Necesario para LocalStack
        aws_secret_access_key='test'           # Necesario para LocalStack
    )

s3 = boto3.client(
        's3',
        region_name='us-east-1',
        endpoint_url='http://localhost:4566',  # <--- ¡ESTA LÍNEA ES VITAL!
        aws_access_key_id='test',              # Necesario para LocalStack
        aws_secret_access_key='test'           # Necesario para LocalStack
    )

def guardar_datalake(data):
    """Guarda el JSON crudo en S3 como un Datalake"""
    try:
        fecha = datetime.datetime.now().strftime("%Y-%m-%d")
        timestamp = str(data.get('timestamp', datetime.datetime.now().timestamp()))
        user_id = str(data.get('userId', 'unknown'))
        
        file_name = f"raw/{fecha}/{user_id}/{timestamp}.json"
        
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=file_name,
            Body=json.dumps(data)
        )
        print(f"[S3] Backup guardado en Datalake: {file_name}")
    except Exception as e:
        print(f"Error guardando en S3: {e}")

def procesar_mensajes():
    print(f"--- QUALITY GATE ACTIVO ---")
    print("Esperando mensajes de RAW...")

    while True:
        response = sqs.receive_message(
            QueueUrl=RAW_URL,
            MaxNumberOfMessages=5,
            WaitTimeSeconds=10
        )

        if 'Messages' in response:
            for msg in response['Messages']:
                body = msg['Body']
                receipt = msg['ReceiptHandle']

                print(f"\n[Recibido]: {body}")

                try:
                    data = json.loads(body)
                    
                    guardar_datalake(data)

                    evento_valido = EventoValoracion(**data)

                    sqs.send_message(
                        QueueUrl=CLEAN_URL,
                        MessageBody=evento_valido.json()
                    )
                    print("APROBADO -> Enviado a Clean Queue")

                except (ValidationError, json.JSONDecodeError) as e:
                    print(f"RECHAZADO: {e}")
                    sqs.send_message(
                        QueueUrl=DLQ_URL,
                        MessageBody=body,
                        MessageAttributes={'Error': {'DataType': 'String', 'StringValue': str(e)}}
                    )
                
                sqs.delete_message(QueueUrl=RAW_URL, ReceiptHandle=receipt)

if __name__ == "__main__":
    procesar_mensajes()
