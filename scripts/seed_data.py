import mysql.connector
import uuid
import random
from datetime import datetime, timedelta

def seed_database():
    conn = mysql.connector.connect(
        host = "127.0.0.1",
        user = "root",
        password = "root",
        database = "tickstream"
    )

    cursor = conn.cursor()