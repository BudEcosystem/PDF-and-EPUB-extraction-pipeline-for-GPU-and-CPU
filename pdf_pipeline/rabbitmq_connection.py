# pylint: disable=all
# type: ignore
import pika
import os
from dotenv import load_dotenv

load_dotenv()

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT"))
RABBITMQ_USERNAME = os.getenv("RABBITMQ_USERNAME")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD")

def get_rabbitmq_connection():
    credentials = pika.PlainCredentials(username=RABBITMQ_USERNAME, password=RABBITMQ_PASSWORD)
    
    # Specify the credentials in the connection parameters
    connection_params = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        # Replace with the appropriate port
        heartbeat=0,
        credentials=credentials
    )
    return pika.BlockingConnection(connection_params)

def get_channel(connection):
    return connection.channel()

if __name__=='__main__':
    connection = get_rabbitmq_connection()
    print("RabbitMQ connection established")
