# pylint: disable=all
# type: ignore
import json
import sys
import cv2
import traceback
import os
sys.path.append("pdf_extraction_pipeline")
import pymongo
from pdf_producer import check_ptm_completion_queue,error_queue
from rabbitmq_connection import get_rabbitmq_connection, get_channel
import layoutparser as lp

connection = get_rabbitmq_connection()
channel = get_channel(connection)

client = pymongo.MongoClient(os.environ['DATABASE_URL'])
db = client.book_set_2
mfd_book_job_details=db.mfd_book_job_details
mfd_done=db.mfd_done


mathformuladetection_model= lp.Detectron2LayoutModel(
                            config_path ="lp://MFD/faster_rcnn_R_50_FPN_3x/config",
                            label_map={1: "Equation"},
                            extra_config=["MODEL.ROI_HEADS.SCORE_THRESH_TEST", 0.8] )


def mathformuladetection_layout(ch, method, properties, body):
    try:
        message = json.loads(body)
        print(message)
        job = message['job']
        total_pages = message['total_pages']
        image_path = message["image_path"]
        page_num = message["page_num"]
        bookname = message["bookname"]
        bookId = message["bookId"]
        existing_page = mfd_book_job_details.find_one({"bookId": bookId, "pages.page_num": page_num})
        if existing_page:
            if total_pages == (page_num + 1):
                check_ptm_completion_queue('check_ptm_completion_queue', bookname, bookId)
            else:
                return
        image = cv2.imread(image_path)
        image = image[..., ::-1] 
        mathformuladetection_layoutds = mathformuladetection_model.detect(image)
        layout_blocks = []
        for item in mathformuladetection_layoutds:  
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
        existing_book = mfd_book_job_details.find_one({"bookId": bookId})
        if existing_book:
            mfd_book_job_details.update_one(
                {"_id": existing_book["_id"]},
                {"$push": {"pages": book_page_data}}
            )
        else:
            new_book_document = {
                "bookId": bookId,
                "bookname": bookname,
                "pages": [book_page_data]
            }
            mfd_book_job_details.insert_one(new_book_document)
        if total_pages == (page_num + 1):
            new_ptm_book_document = {
                "bookId": bookId,
                "bookname": bookname,
                "status": "MFD done"
            }
            mfd_done.insert_one(new_ptm_book_document)
            check_ptm_completion_queue('check_ptm_completion_queue', bookname, bookId)
            print("hello world ")
    except Exception as e:
        error = {"consumer":"mfd_consumer","consumer_message":message,"page_num":page_num, "error":str(e), "line_number":traceback.extract_tb(e.__traceback__)[-1].lineno} 
        print(print(error))
        error_queue('error_queue',bookname, bookId, error)
    finally:
        ch.basic_ack(delivery_tag=method.delivery_tag)


def consume_mfd_queue():
    try:
        channel.basic_qos(prefetch_count=1, global_qos=False)

        channel.queue_declare(queue='mfd_queue')
        # Set up the callback function for handling messages from the queue
        channel.basic_consume(queue='mfd_queue', on_message_callback=mathformuladetection_layout)

        print(' [*] Waiting for messages on mfd_queue. To exit, press CTRL+C')
        channel.start_consuming()

    except KeyboardInterrupt:
        pass
    finally:
        channel.close()
        connection.close()

   

if __name__ == "__main__":
    try:
        consume_mfd_queue()
    except KeyboardInterrupt:
        pass
