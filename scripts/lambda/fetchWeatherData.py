import json
import boto3
import urllib3
from datetime import datetime


def lambda_handler(event, context):
    
    BUCKET_NAME = 'data-store-sundeep'
    API_KEY = '<API_KEY>'
    COUNTRY = 'singapore'
    API_URL = f'https://api.openweathermap.org/data/2.5/weather?q={COUNTRY}&appid={API_KEY}'
    
    try:
        print("Fetching the data and Saving into S3")
        http = urllib3.PoolManager()
        response = http.request('GET', API_URL)
        if response.status == 200:
            data_raw = response.data
            data = data_raw.decode('utf-8')
            json_data = json.loads(data)
            filtered_json = parse_json_data(json_data)
            
            print("Original JSON: ", json_data)
            print("filtered JSON: ", filtered_json)
            
            filtered_json_encoded = json.dumps(filtered_json)
            
            utc_date = json_data.get('dt', 1337963801)
            date_time = datetime.utcfromtimestamp(utc_date)
            file_name = f'{date_time.year}-{date_time.month}-{date_time.day}.json'
            raw_folder_name = "raw/weather"
            filtered_folder_name = "filtered/weather"
            raw_s3_path = "{}/{}".format(raw_folder_name, file_name)
            filtered_s3_path = "{}/{}".format(filtered_folder_name, file_name)
        
            s3 = boto3.client('s3')
            s3.put_object(Bucket=BUCKET_NAME, Key=raw_s3_path, Body=data_raw)
            s3.put_object(Bucket=BUCKET_NAME, Key=filtered_s3_path, Body=filtered_json_encoded)
            
            print("Done")
    except Exception as ex:
        print("Exception: ", ex)
    

    print("Upload completed!!")


def parse_json_data(data):
    """
    Weather Condition Status Codes: https://openweathermap.org/weather-conditions
    """
    filtered_json = {}
    for _data in data['weather']:
        if _data['id'] // 100 in [2, 3, 5]:
            filtered_json['rain'] = True
            break
    else:
        filtered_json['rain'] = False
    
    filtered_json['temp'] = data['main']['temp']
    filtered_json['temp_min'] = data['main']['temp_min']
    filtered_json['temp_max'] = data['main']['temp_max']
    date_time = datetime.utcfromtimestamp(data['dt'])
    filtered_json['year'] = date_time.year
    filtered_json['month'] = date_time.month
    filtered_json['day'] = date_time.day
    return filtered_json
    
