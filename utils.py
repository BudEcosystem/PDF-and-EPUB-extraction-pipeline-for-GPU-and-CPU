import time
from functools import wraps
import cv2

def timeit(func):
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print(f'Function {func.__name__} Took {total_time:.4f} seconds')
        return result
    return timeit_wrapper

def crop_image(block, imagepath, id):
    x1, y1, x2, y2 = block['x_1'], block['y_1'], block['x_2'], block['y_2']
    img = cv2.imread(imagepath)
    # Expand the bounding box by 5 pixels on every side
    x1-=5
    y1-=5
    x2+=5
    y2+=5

    # Ensure the coordinates are within the image boundaries
    x1=max(0,x1)
    y1=max(0,y1)
    x2=min(img.shape[1],x2)
    y2=min(img.shape[0],y2)

    #crop the expanded bounding box
    bbox = img[int(y1):int(y2), int(x1):int(x2)]
    cropped_image_path = f"cropeed{id}.jpg"
    cv2.imwrite(cropped_image_path,bbox) 

    return cropped_image_path 
