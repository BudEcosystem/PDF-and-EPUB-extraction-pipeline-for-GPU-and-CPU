# pylint: disable=all
# type: ignore
import json
import sys
import traceback
sys.path.append("pdf_extraction_pipeline")
from pdf_producer import send_to_queue, error_queue
import layoutparser as lp
from utils import (
    timeit,
    read_image_from_str,
    get_mongo_collection,
    get_rabbitmq_connection,
    get_channel
)

QUEUE_NAME = "table_bank_queue"

connection = get_rabbitmq_connection()
channel = get_channel(connection)

table_bank_pages = get_mongo_collection('table_bank_pages')

tablebank_model = lp.Detectron2LayoutModel('lp://TableBank/faster_rcnn_R_50_FPN_3x/config',
                                 extra_config=["MODEL.ROI_HEADS.SCORE_THRESH_TEST", 0.8],
                                 label_map={0: "Table"})

@timeit
def table_bank_layout(ch, method, properties, body):
    message = json.loads(body)
    image_path = message["image_path"]
    page_num = message["page_num"]
    # book_path = message["split_path"]
    bookId = message["bookId"]
    image_str = message["image_str"]
    print(f"Received message for {image_path}")
    queue_msg = {
        "bookId": bookId,
        # "split_path": book_path,
        "page_num": page_num
    }
    try:
        existing_page = table_bank_pages.find_one({
            "bookId": bookId,
            "pages.page_num": page_num
        })
        if existing_page:
            send_to_queue('check_ptm_completion_queue', queue_msg)
            return
        image = read_image_from_str(image_str)
        image = image[..., ::-1]
        tablebank_layouts = tablebank_model.detect(image)
        layout_blocks = []
        for item in tablebank_layouts:  
            output_item = {
                "x_1": item.block.x_1,
                "y_1": item.block.y_1,
                "x_2": item.block.x_2,
                "y_2": item.block.y_2,
                'type': item.type
            }
            layout_blocks.append(output_item)
        book_page_data = {
            'page_num': page_num,
            'image_path': image_path,
            'status': 'done',
            'result': layout_blocks
        }
        existing_book = table_bank_pages.find_one({"bookId": bookId})
        if existing_book:
            table_bank_pages.update_one(
                {"_id": existing_book["_id"]},
                {"$push": {"pages": book_page_data}}
            )
        else:
            new_book_document = {
                "bookId": bookId,
                "pages": [book_page_data]
            }
            table_bank_pages.insert_one(new_book_document)
        send_to_queue('check_ptm_completion_queue', queue_msg)
    except Exception as e:
        print(traceback.format_exc())
        error = {
            "consumer": QUEUE_NAME,
            "consumer_message": message,
            "page_num": page_num,
            "error": str(e),
            "line_number":traceback.extract_tb(e.__traceback__)[-1].lineno
        }
        error_queue('', bookId, error)
    finally:
        ch.basic_ack(delivery_tag=method.delivery_tag)



def consume_table_bank_queue():
    channel.basic_qos(prefetch_count=1, global_qos=False)
    # Declare the queue
    channel.queue_declare(queue=QUEUE_NAME)

    # Set up the callback function for handling messages from the queue
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=table_bank_layout)

    print(f' [*] Waiting for messages on {QUEUE_NAME}. To exit, press CTRL+C')
    channel.start_consuming()


if __name__ == "__main__":
    try:
        consume_table_bank_queue()
    except KeyboardInterrupt:
        pass
    finally:
        connection.close()