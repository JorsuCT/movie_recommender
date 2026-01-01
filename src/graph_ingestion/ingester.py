import boto3
import json
import os
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()
CLEAN_URL = os.getenv('MOVIE-QUEUE-CLEAN_URL')

NEO_URI = "bolt://localhost:7687"
NEO_AUTH = ("neo4j", "TSCD2025") 

if not CLEAN_URL:
    raise Exception("ERROR: No se encuentra MOVIE-QUEUE-CLEAN_URL en .env")

#sqs = boto3.client('sqs', region_name = 'us-east-1')
sqs = boto3.client(
        'sqs',
        region_name='us-east-1',
        endpoint_url='http://localhost:4566',  # <--- ¡ESTA LÍNEA ES VITAL!
        aws_access_key_id='test',              # Necesario para LocalStack
        aws_secret_access_key='test'           # Necesario para LocalStack
    )

driver = GraphDatabase.driver(NEO_URI, auth = NEO_AUTH, encrypted = False)

def escribir_en_grafo(data):

    """
    Ejecuta una transacción Cypher para guardar el voto.
    Usa MERGE para no duplicar datos si el mensaje llega dos veces.
    """

    query = """
    MATCH (m:Pelicula {movieId: $movieId})
    MERGE (u:Usuario {userId: $userId})
    MERGE (u)-[r:VIO]->(m)
    SET r.rating = $rating, 
        r.timestamp = $timestamp,
        r.fecha_procesado = datetime()
    RETURN u, m
    """
    
    with driver.session() as session:
        result = session.run(query, 
                             movieId = data['movieId'], 
                             userId = data['userId'], 
                             rating = data['rating'],
                             timestamp = data['timestamp'])
        
        record = result.peek()
        if record:
            print(f"[NEO4J] Guardado: User {data['userId']} -> Movie {data['movieId']}")
        else:
            print(f"[NEO4J] Alerta: Película {data['movieId']} no encontrada en BD. Voto ignorado.")

def procesar_cola_limpia():
    print(f"--- GRAPH INGESTER ACTIVO ---")
    print(f"Escuchando: {CLEAN_URL}")
    print("Conectado a Neo4j en localhost:7687")

    while True:
        response = sqs.receive_message(
            QueueUrl = CLEAN_URL,
            MaxNumberOfMessages = 10,
            WaitTimeSeconds = 10
        )

        if 'Messages' in response:
            for msg in response['Messages']:
                body = msg['Body']
                receipt = msg['ReceiptHandle']
                
                try:
                    data = json.loads(body)
                    escribir_en_grafo(data)
                    
                    sqs.delete_message(QueueUrl = CLEAN_URL, ReceiptHandle = receipt)
                    
                except Exception as e:
                    print(f"Error procesando mensaje: {e}")

if __name__ == "__main__":
    try:
        procesar_cola_limpia()
    except Exception as e:
        print(f"\nError Crítico: {e}")
        print("Consejo: Revisa si Docker está corriendo y la contraseña de Neo4j es correcta.")
    finally:
        driver.close()