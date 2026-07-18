import json 
from confluent_kafka import Consumer

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'tickstream-workers', 
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['trades.raw'])

print("Worker started. Listening for trades...")

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(f"Error: {msg.error()}")
        continue
    
    raw_bytes = msg.value()
    trade_string = raw_bytes.decode('utf-8')
    trade_data = json.loads(trade_string)
    print(f"Processed trade: {trade_data['qty']} {trade_data['symbol']} @ ${trade_data['price']}")
