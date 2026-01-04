import pytest
from fastapi.testclient import TestClient
import boto3
import json
import os
import time
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src')))
from producer.api import app

client = TestClient(app)

SQS_URL = "http://localhost:4566/000000000000/movie-queue-raw"

def get_sqs_client():
    return boto3.client('sqs', 
                        region_name='us-east-1', 
                        endpoint_url='http://localhost:4566',
                        aws_access_key_id='test', 
                        aws_secret_access_key='test')

def test_health_check():
    """Verifica que la API está viva"""
    response = client.get("/docs")
    assert response.status_code == 200

def test_prometheus_metrics():
    """Verifica que Prometheus está exponiendo métricas"""
    response = client.get("/metrics")
    assert response.status_code == 200
    assert "http_requests_total" in response.text

def test_flujo_completo_api_a_sqs():
    """
    Test de Integración:
    Envia un rating a la API y verifica que aparece en la cola SQS de LocalStack.
    """
    sqs = get_sqs_client()
    try:
        sqs.purge_queue(QueueUrl=SQS_URL)
    except:
        pass 

    payload = {"userId": 777, "movieId": 1, "rating": 5.0}
    response = client.post("/rate", json=payload)
    
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "message": "Rating enviado"}

    time.sleep(1)
    mensajes = sqs.receive_message(QueueUrl=SQS_URL, MaxNumberOfMessages=1, WaitTimeSeconds=2)

    assert 'Messages' in mensajes, "Fallo: El mensaje no llegó a la cola SQS"
    
    body = json.loads(mensajes['Messages'][0]['Body'])
    assert body['userId'] == 777
    assert body['rating'] == 5.0