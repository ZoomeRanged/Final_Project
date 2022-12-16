from kafka import KafkaProducer
import requests
import json
import datetime
import time


# assign currency pair
currency_pair = {'EURUSD': 'US Dollar',
                'EURGBP': 'Pound Sterling',
                'USDEUR': 'Euro'}


# create function to get result from API
def get_result(currency):
    a = requests.Session()
    url = 'https://www.freeforexapi.com/api/live?pairs=EURUSD,EURGBP,USDEUR'.format(currency)
    response = a.get(url, headers=None, stream=True).json()
    return response

# create function to convert the result into final dictionary
def extract(input, cur_id, cur_name): 
    result_dict = {}
    result_dict['currency_id'] = cur_id
    result_dict['currency_name'] = cur_name
    
    for key, value in input.items():
        for k, v in value.items():
            if k == 'rate':
                result_dict[k] = v
            elif k == 'timestamp':
                result_dict[k] = datetime.datetime.fromtimestamp(v).strftime('%Y-%m-%d %H:%M:%S')

    return result_dict


# connect to Kafka Producer
producer = KafkaProducer(bootstrap_servers=['kafka:9092'], api_version=(0, 10), value_serializer=lambda v: str(v).encode("utf-8"))


# execute the API extraction
while True:
    for key, value in currency_pair.items():
        link = dict(get_result(key))['rates']
        task = extract(link, key, value)
        producer.flush()
        producer.send('TopicCurrency', json.dumps(task))
    time.sleep(60)