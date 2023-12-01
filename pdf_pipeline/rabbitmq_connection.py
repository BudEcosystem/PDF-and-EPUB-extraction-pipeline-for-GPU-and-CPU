import pika

def get_rabbitmq_connection():
    connection_params = pika.ConnectionParameters('localhost', heartbeat=0)
    return pika.BlockingConnection(connection_params)

def get_channel(connection):
    return connection.channel()
