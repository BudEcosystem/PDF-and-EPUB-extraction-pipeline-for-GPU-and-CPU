# import os

# directory_path = os.path.dirname(os.path.abspath(__file__))
# print(directory_path)
# img_dir = directory_path+'/../images'
# if not os.path.exists(img_dir):
#     os.makedirs(img_dir)
#     print("pdf_pipeline directory exists")


import requests
import json

def send_data_to_api(data):
    api_url = 'http://localhost:9000'

    # Set the headers for the request
    headers = {'Content-Type': 'application/json'}

    try:
        # Convert Python data to JSON
        json_data = json.dumps(data)

        # Make a POST request to the API
        response = requests.post(api_url, data=json_data, headers=headers)

        # Check if the request was successful (status code 2xx)
        if response.status_code==200:
            print(response.json())
        else:
            print(f'Error: {response.status_code} - {response.text}')

    except Exception as e:
        print(f'Error: {str(e)}')

# Example data to be sent
data_to_send = {
    "math":"<math alttext=""><mrow><mi>f</mi><mrow><mo>(</mo><mi>x</mi><mo>)</mo></mrow><mo>=</mo><msup><mi>x</mi> <mn>2</mn> </msup></mrow></math>"
}

# Call the function with the data
send_data_to_api(data_to_send)

