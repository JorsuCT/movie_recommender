import boto3
import sys

# Nombres de las colas que necesitamos
QUEUE_NAMES = [
    "movie-queue-raw",
    "movie-queue-clean",
    "movie-queue-dlq"
]

BUCKET_NAME = "movie-datalake"

def setup_infrastructure():
    print("INICIANDO DESPLIEGUE DE INFRAESTRUCTURA AWS")
    
    # Cliente de SQS
    try:
        sqs = boto3.client('sqs', region_name='us-east-1', endpoint_url='http://localhost:4566', aws_access_key_id='test', aws_secret_access_key='test')
        s3 = boto3.client('s3', region_name='us-east-1', endpoint_url='http://localhost:4566', aws_access_key_id='test', aws_secret_access_key='test')
        sqs.list_queues()
        print("--> Conectado a entorno LOCAL (Docker/LocalStack)")
    except Exception as e:
        print("Error: No se detectan credenciales de AWS.")
        sys.exit(1)

    urls = {}

    for q_name in QUEUE_NAMES:
        try:
            print(f"Creando/Verificando cola: {q_name}...")
            
            response = sqs.create_queue(QueueName=q_name)
            
            queue_url = response['QueueUrl']
            urls[q_name] = queue_url
            print(f"URL: {queue_url}")
            
        except Exception as e:
            print(f"Error creando {q_name}: {e}")

    try: 
        s3.create_bucket(Bucket=BUCKET_NAME)
    except Exception as e:
        print(f"Aviso S3: {e}")

    print("\nINFRAESTRUCTURA LISTA.")
    print("Copia estas URLs en tu archivo de configuración o variables de entorno:")
    print("-" * 50)
    for name, url in urls.items():
        print(f"{name.upper()}_URL = '{url}'")
    print("-" * 50)

    with open(".env", "w") as f:
        for name, url in urls.items():
            f.write(f"{name.upper()}_URL={url}\n")
            
        bucket_line = f"DATALAKE_BUCKET={BUCKET_NAME}"
        f.write(f"{bucket_line}\n")
        
    print("Se ha generado un archivo '.env' con las URLs automáticamente.")

if __name__ == "__main__":
    setup_infrastructure()
