import json 
from confluent_kafka import Producer

conf = {
    'bootstrap.servers': 'localhost:9092',
    'acks': 'all'
}

producer = Producer(conf)

trade = {"symbol" : "BTC", "price" : 65000.50 , "qty" : 1.2}

trade_bytes = json.dumps(trade).encode('utf-8')

producer.produce(topic = 'trades.raw', value = trade_bytes)

producer.flush()
print("Trade sent successfully")
