# pylint: disable=all
# type: ignore
import json
import cv2
from PIL import Image
import traceback
from pdf_pipeline.pdf_producer import send_to_queue, error_queue

# import layoutparser as lp
from docxtract import LayoutAnalysisPipeline
from utils import (
    timeit,
    get_mongo_collection,
    get_rabbitmq_connection,
    get_channel,
    get_gpu_device_id,
)

Image.MAX_IMAGE_PIXELS = None
QUEUE_NAME = "ptm_queue"

connection = get_rabbitmq_connection()
channel = get_channel(connection)

ptm_pages = get_mongo_collection("ptm_pages")
index_name = "index_bookId_pageNo"
indexes_info = ptm_pages.list_indexes()
index_exists = any(index_info["name"] == index_name for index_info in indexes_info)
if not index_exists:
    ptm_pages.create_index(
        ["bookId", "pages.page_num"], name=index_name, background=True
    )


# publaynet_model = lp.Detectron2LayoutModel('lp://PubLayNet/mask_rcnn_X_101_32x8d_FPN_3x/config',
#                                  extra_config=["MODEL.ROI_HEADS.SCORE_THRESH_TEST", 0.8],
#                                  label_map= {0: "Text", 1: "Title", 2: "List", 3: "Table", 4: "Figure"})

# tablebank_model = lp.Detectron2LayoutModel('lp://TableBank/faster_rcnn_R_50_FPN_3x/config',
#                                  extra_config=["MODEL.ROI_HEADS.SCORE_THRESH_TEST", 0.8],
#                                  label_map={0: "Table"})

# mathformuladetection_model= lp.Detectron2LayoutModel(
#                             config_path ="lp://MFD/faster_rcnn_R_50_FPN_3x/config",
#                             label_map={1: "Equation"},
#                             extra_config=["MODEL.ROI_HEADS.SCORE_THRESH_TEST", 0.8] )
device_id = get_gpu_device_id()
device = f"cuda:{device_id}"
publaynet_model = LayoutAnalysisPipeline.from_pretrained(
    pretrained_model_name_or_path="publaynet", device=device
)
tablebank_model = LayoutAnalysisPipeline.from_pretrained(
    pretrained_model_name_or_path="tablebank", device=device
)
mathformuladetection_model = LayoutAnalysisPipeline.from_pretrained(
    pretrained_model_name_or_path="mfd", device=device
)


@timeit
def ptm_layout(ch, method, properties, body):
    message = json.loads(body)
    # image_path = message["image_path"]
    # page_num = message["page_num"]
    image_paths = message["image_path"]
    page_nums = message["page_num"]
    # book_path = message["split_path"]
    bookId = message["bookId"]
    # image_str = message["image_str"]
    print(f"Received message for {image_paths}")
    # queue_msg = {
    #     "bookId": bookId,
    #     # "split_path": book_path,
    #     "page_num": page_num
    # }
    try:
        # existing_page = ptm_pages.find_one({
        #     "bookId": bookId,
        #     "pages.page_num": page_num
        # })
        # if existing_page:
        #     send_to_queue('check_ptm_completion_queue', queue_msg)
        #     return
        # image = read_image_from_str(image_str)
        # layout_blocks = get_layout(image_path)
        for img_path, page_num, layout_blocks in get_layout_docxtract(
            image_paths, page_nums
        ):
            queue_msg = {"bookId": bookId, "page_num": page_num}
            book_page_data = {
                "page_num": page_num,
                "image_path": img_path,
                "status": "done",
                "result": layout_blocks,
            }
            existing_book = ptm_pages.find_one({"bookId": bookId})
            if existing_book:
                ptm_pages.update_one(
                    {"_id": existing_book["_id"]}, {"$push": {"pages": book_page_data}}
                )
            else:
                new_book_document = {"bookId": bookId, "pages": [book_page_data]}
                ptm_pages.insert_one(new_book_document)
            send_to_queue("check_ptm_completion_queue", queue_msg)
    except Exception as e:
        print(traceback.format_exc())
        error = {
            "consumer": QUEUE_NAME,
            "consumer_message": message,
            "page_num": page_nums,
            "error": str(e),
            "line_number": traceback.extract_tb(e.__traceback__)[-1].lineno,
        }
        error_queue("", bookId, error)
    finally:
        ch.basic_ack(delivery_tag=method.delivery_tag)


def get_layout(image_path):
    layout_blocks = []
    try:
        image = cv2.imread(image_path)
        image = image[..., ::-1]
        publaynet_layouts = publaynet_model.detect(image)
        tablebank_layouts = tablebank_model.detect(image)
        mathformuladetection_layoutds = mathformuladetection_model.detect(image)
        for item in publaynet_layouts:
            if item.type != "Table":
                output_item = {
                    "x_1": item.block.x_1,
                    "y_1": item.block.y_1,
                    "x_2": item.block.x_2,
                    "y_2": item.block.y_2,
                    "type": item.type,
                    "image_path": image_path,
                }
                layout_blocks.append(output_item)
        for item in tablebank_layouts:
            output_item = {
                "x_1": item.block.x_1,
                "y_1": item.block.y_1,
                "x_2": item.block.x_2,
                "y_2": item.block.y_2,
                "type": item.type,
                "image_path": image_path,
            }
            layout_blocks.append(output_item)
        for item in mathformuladetection_layoutds:
            output_item = {
                "x_1": item.block.x_1,
                "y_1": item.block.y_1,
                "x_2": item.block.x_2,
                "y_2": item.block.y_2,
                "type": item.type,
                "image_path": image_path,
            }
            layout_blocks.append(output_item)
    except cv2.error as excep:
        print(excep)
    return layout_blocks


def get_layout_docxtract(image_paths, page_nums):
    images = [Image.open(img_path) for img_path in image_paths]
    pub_layout = publaynet_model(images=images)
    publaynet_layouts = [each for each in pub_layout]
    images = [Image.open(img_path) for img_path in image_paths]
    tb_layout = tablebank_model(images=images)
    tablebank_layouts = [each for each in tb_layout]
    images = [Image.open(img_path) for img_path in image_paths]
    mfd_layout = mathformuladetection_model(images=images)
    mathformuladetection_layouts = [each for each in mfd_layout]
    for i, img_path in enumerate(image_paths):
        layout_blocks = []
        page_num = page_nums[i]
        pub = publaynet_layouts[i]
        tb = tablebank_layouts[i]
        mfd = mathformuladetection_layouts[i]
        for item in pub:
            if item["label"] != "Table":
                output_item = {
                    "x_1": item["xmin"],
                    "y_1": item["ymin"],
                    "x_2": item["xmax"],
                    "y_2": item["ymax"],
                    "type": item["label"],
                    "image_path": img_path,
                }
                layout_blocks.append(output_item)
        for item in tb:
            output_item = {
                "x_1": item["xmin"],
                "y_1": item["ymin"],
                "x_2": item["xmax"],
                "y_2": item["ymax"],
                "type": item["label"],
                "image_path": img_path,
            }
            layout_blocks.append(output_item)
        for item in mfd:
            output_item = {
                "x_1": item["xmin"],
                "y_1": item["ymin"],
                "x_2": item["xmax"],
                "y_2": item["ymax"],
                "type": item["label"],
                "image_path": img_path,
            }
            layout_blocks.append(output_item)
        yield img_path, page_num, layout_blocks


def consume_publaynet_queue():
    channel.basic_qos(prefetch_count=1, global_qos=False)
    # Declare the queue
    channel.queue_declare(queue=QUEUE_NAME)

    # Set up the callback function for handling messages from the queue
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=ptm_layout)

    print(f" [*] Waiting for messages on {QUEUE_NAME}. To exit, press CTRL+C")
    channel.start_consuming()


if __name__ == "__main__":
    try:
        consume_publaynet_queue()
    except KeyboardInterrupt:
        pass
    finally:
        # connection.close()
        pass
