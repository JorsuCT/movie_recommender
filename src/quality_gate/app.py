import boto3
import json
import os
import sys
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.common.schemas import EventoValoracion
from pydantic import ValidationError

load_dotenv()

RAW_URL = os.getenv('MOVIE-QUEUE-RAW_URL')
CLEAN_URL = os.getenv('MOVIE-QUEUE-CLEAN_URL')
DLQ_URL = os.getenv('MOVIE-QUEUE-DLQ_URL')

sqs = boto3.client('sqs', region_name='us-east-1')

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
