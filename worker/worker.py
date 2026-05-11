import asyncio
from aiokafka import AIOKafkaConsumer
import json
import psycopg2

DB_CONFIG = {
    "host": "postgres", 
    "database": "log_db",
    "user": "myuser",
    "password": "mypassword"
}
KAFKA_SERVER = "kafka:9092" 

def save_to_db(data):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        # إنشاء الجدول إذا لم يكن موجوداً
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sensor_logs (
                id SERIAL PRIMARY KEY,
                sensor_name TEXT,
                reading FLOAT,
                raw_data JSONB
            );
        """)
        # إدخال البيانات
        cur.execute(
            "INSERT INTO sensor_logs (sensor_name, reading, raw_data) VALUES (%s, %s, %s)",
            (data.get("sensor"), data.get("reading"), json.dumps(data))
        )
        conn.commit()
        cur.close()
        conn.close()
        print("✅ Data saved to PostgreSQL!")
    except Exception as e:
        print(f"❌ Database Error: {e}")

async def consume():
    consumer = AIOKafkaConsumer(
        'sensor-topic',
        bootstrap_servers='localhost:9092',
        group_id="log-processors-group"
    )
    await consumer.start()
    print("Worker is live and saving to DB...")
    try:
        async for msg in consumer:
            data = json.loads(msg.value.decode('utf-8'))
            print(f"🔥 Processing: {data}")
            save_to_db(data)
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(consume())