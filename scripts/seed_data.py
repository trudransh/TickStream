import mysql.connector
import uuid
import random
from datetime import datetime, timedelta

def seed_database():
    # 1. Connect to the Docker MySQL container
    conn = mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password="root",
        database="tickstream"
    )
    cursor = conn.cursor()

    insert_query = """
        INSERT INTO trades (event_uuid, symbol, ts_event, ts_ingest, price, qty)
        VALUES (%s, %s, %s, %s, %s, %s)
    """

    total_rows = 1_000_000
    batch_size = 10_000
    
    print("Starting 1M row generation... 🐍")

    for i in range(0, total_rows, batch_size):
        batch = []
        for _ in range(batch_size):
            trade = (
                str(uuid.uuid4()),
                random.choice(['BTC-USD', 'ETH-USD', 'SOL-USD']),
                datetime.now() - timedelta(minutes=random.randint(0, 1000)),
                datetime.now(),
                round(random.uniform(50, 60000), 2),
                round(random.uniform(0.1, 10.0), 4)
            )
            batch.append(trade)
        
        # 2. Execute the batch!
        cursor.executemany(insert_query, batch)
        conn.commit() # Save the batch to disk
        print(f"Inserted {i + batch_size} / {total_rows} rows")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    seed_database()