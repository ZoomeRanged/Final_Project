from kafka import KafkaProducer
from forex_python.converter import CurrencyRates

producer = KafkaProducer(bootstrap_servers=["localhost:9092"])
cr = CurrencyRates()

# Replace USD and EUR with the desired currency codes
usd_eur_rate = cr.get_rate("USD", "EUR")

# Replace "forex-rates" with the desired topic name
producer.send("TopicCurrency", usd_eur_rate)

print(usd_eur_rate)