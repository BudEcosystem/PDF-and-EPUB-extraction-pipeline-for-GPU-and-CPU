# pylint: disable=all
# type: ignore
import json
import sys
sys.path.append("pdf_extraction_pipeline")
import cv2
import os
import traceback
from utils import timeit
import pymongo
from pdf_producer import check_ptm_completion_queue, error_queue
from rabbitmq_connection import get_rabbitmq_connection, get_channel
import layoutparser as lp

connection = get_rabbitmq_connection()
channel = get_channel(connection)


client = pymongo.MongoClient(os.environ['DATABASE_URL'])
db = client.bookssssss
error_collection = db.error_collection
publaynet_book_job_details=db.publaynet_book_job_details
publaynet_done=db.publaynet_done



publaynet_model = lp.Detectron2LayoutModel('lp://PubLayNet/mask_rcnn_X_101_32x8d_FPN_3x/config',
                                 extra_config=["MODEL.ROI_HEADS.SCORE_THRESH_TEST", 0.8],
                                 label_map= {0: "Text", 1: "Title", 2: "List", 3: "Table", 4: "Figure"})

@timeit
def publaynet_layout(ch, method, properties, body):
    try:
        message = json.loads(body)
        print(message)
        job = message['job']
        total_pages = message['total_pages']
        image_path = message["image_path"]
        page_num = message["page_num"]
        bookname = message["bookname"]
        bookId = message["bookId"]

        image = cv2.imread(image_path)
        image = image[..., ::-1] 
        publaynet_layouts = publaynet_model.detect(image)
        layout_blocks = []
        for item in publaynet_layouts:
            if item.type != "Table":
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
            "job": job,
            'image_path': image_path,
            'status': 'done',
            'result': layout_blocks
        }
        existing_book = publaynet_book_job_details.find_one({"bookId": bookId})
        if existing_book:
            publaynet_book_job_details.update_one(
                {"_id": existing_book["_id"]},
                {"$push": {"pages": book_page_data}}
            )
        else:
            new_book_document = {
                "bookId": bookId,
                "bookname": bookname,
                "pages": [book_page_data]
            }
            publaynet_book_job_details.insert_one(new_book_document)

        if total_pages == (page_num + 1):
            new_ptm_book_document = {
                "bookId": bookId,
                "bookname": bookname,
                "ptm": "PubLaynet done"
            }
            publaynet_done.insert_one(new_ptm_book_document)
            check_ptm_completion_queue('check_ptm_completion_queue', bookname, bookId)

    except Exception as e:
        error = {"page_num":page_num,"error":str(e), "line_number":traceback.extract_tb(e.__traceback__)[-1].lineno} 
        print(print(error))
        error_queue('error_queue','publeynet_consumer',bookname, bookId, error)

    finally:
        ch.basic_ack(delivery_tag=method.delivery_tag)



def consume_publaynet_queue():
    try:
        # Declare the queue
        channel.queue_declare(queue='publeynet_queue')

        # Set up the callback function for handling messages from the queue
        channel.basic_consume(queue='publeynet_queue', on_message_callback=publaynet_layout)

        print(' [*] Waiting for messages on publeynet_queue. To exit, press CTRL+C')
        channel.start_consuming()

    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    try:
        consume_publaynet_queue()     
    except KeyboardInterrupt:
        pass
