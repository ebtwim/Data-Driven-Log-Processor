import asyncio
from aiokafka import AIOKafkaConsumer
import json
import psycopg2

# إعدادات قاعدة البيانات - نستخدم اسم الخدمة "postgres" للربط داخل Docker
DB_CONFIG = {
    "host": "postgres", 
    "database": "log_db",
    "user": "myuser",
    "password": "mypassword"
}

# عنوان كافكا - نستخدم اسم الخدمة "kafka" للربط داخل Docker
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
        # إدخال البيانات - تأكد من مطابقة الأسماء في JSON (sensor)
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
    # التصحيح هنا: استبدلنا 'localhost:9092' بالمتغير KAFKA_SERVER الذي يحتوي على 'kafka:9092'
    consumer = AIOKafkaConsumer(
        'sensor-topic',
        bootstrap_servers=KAFKA_SERVER,
        group_id="log-processors-group",
        # إضافة اختيارية لضمان إعادة الاتصال إذا كان كافكا لا يزال يبدأ
        retry_backoff_ms=500 
    )
    
    # محاولة بدء التشغيل مع معالجة الأخطاء إذا لم يكن كافكا جاهزاً بعد
    while True:
        try:
            await consumer.start()
            break
        except Exception as e:
            print(f"Waiting for Kafka to be ready... ({e})")
            await asyncio.sleep(2)

    print("🚀 Worker is live and saving to DB...")
    try:
        async for msg in consumer:
            data = json.loads(msg.value.decode('utf-8'))
            print(f"🔥 Processing: {data}")
            save_to_db(data)
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(consume())