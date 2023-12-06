import pika
def delete_all_messages_from_queue(queue_name):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()

        # Purge all messages from the queue
        purged_messages = channel.queue_purge(queue=queue_name)

        print(f"Deleted {purged_messages} messages from the {queue_name} queue.")

    finally:
        connection.close()

if __name__ == "__main__":
    queue_names = ['pdf_processing_queue', 'publeynet_queue', 'table_bank_queue', 'mfd_queue', 'pdfigcap_queue', 'nougat_queue', 'check_ptm_completion_queue', 'book_completion_queue','page_extraction_queue']
    # queue_names=['pdf_processing_queue']
    for queue_name in queue_names:
        delete_all_messages_from_queue(queue_name)
    