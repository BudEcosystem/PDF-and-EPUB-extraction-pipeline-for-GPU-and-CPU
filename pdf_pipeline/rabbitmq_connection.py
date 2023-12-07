# pylint: disable=all
# type: ignore
import pika

def get_rabbitmq_connection():
    credentials = pika.PlainCredentials(username='guest', password='guest')
    
    # Specify the credentials in the connection parameters
    connection_params = pika.ConnectionParameters(
        host='localhost',
        port=56722,
        # Replace with the appropriate port
        heartbeat=0,
        credentials=credentials
    )
    return pika.BlockingConnection(connection_params)

def get_channel(connection):
    return connection.channel()
