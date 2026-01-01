from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import boto3
import json
import os
import time
from dotenv import load_dotenv
from prometheus_fastapi_instrumentator import Instrumentator

load_dotenv()

app = FastAPI(title="Movie Recomender Ingestion API")

Instrumentator().instrument(app).expose(app)

SQS_URL = os.getenv('MOVIE-QUEUE-RAW_URL')

if not SQS_URL:
    raise Exception("ERROR: No se encuentra MOVIE-QUEUE-RAW_URL en el archivo .env")

sqs = boto3.client('sqs', region_name='us-east-1', endpoint_url='http://localhost:4566', aws_access_key_id='test', aws_secret_access_key='test')

class RatingEvent(BaseModel):
    userId: int
    movieId: int
    rating: float
    
@app.post("/rate")
async def rate_movie(event: RatingEvent):
    """
    Endpoint que recibe una valoracion de una pelicula y la envia a la cola SQS Raw
    """
    
    payload = event.dict()
    payload['timestamp'] = time.time()
    
    if not SQS_URL:
        raise HTTPException(status_code=500, detail="Falta configuración MOVIE-QUEUE-RAW_URL en .env")
    
    try:
        sqs.send_message(
            QueueUrl=SQS_URL,
            MessageBody=json.dumps(payload)
        )
        return {"status": "ok", "message":"Rating enviado"}
    except Exception as e:
        print(f"❌ ERROR CRÍTICO: {str(e)}")
        print(f"Tipo de error: {type(e)}")
        raise HTTPException(status_code=500, detail=str(e))