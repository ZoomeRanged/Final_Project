from kafka import KafkaProducer
import requests

# Create a Kafka producer
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"], api_version=(0, 10),
    value_serializer=lambda v: str(v).encode("utf-8"),
)

# Fetch data from the API
response = requests.get('https://www.freeforexapi.com/api/live?pairs=EURUSD,EURGBP,USDEUR')

# Send the data to the "my-topic" topic
for line in response.iter_lines():
    producer.send("TopicCurrency", value=line)
    print(line)