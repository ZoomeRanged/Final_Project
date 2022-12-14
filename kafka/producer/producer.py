from kafka.producer import KafkaProducer
import json
import requests
import datetime
import time


currency_pair = {'EURUSD': 'US Dollar',
                'EURGBP': 'Pound Sterling',
                'USDEUR': 'Euro'}


# create function to get result from API
def get_result(currency):
    url = 'https://www.freeforexapi.com/api/live?pairs={}'.format(currency)
    response = requests.get(url).json()
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
bootstrap = ['localhost:9092']
producer = KafkaProducer(bootstrap_servers=bootstrap)


# execute the API extraction
while True:
    for key, value in currency_pair.items():
        link = dict(get_result(key))['rates']
        task = extract(link, key, value)
        producer.send('TopicCurrency', task)
    time.sleep(10)