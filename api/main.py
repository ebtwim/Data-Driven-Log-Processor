from fastapi import FastAPI, HTTPException
from aiokafka import AIOKafkaProducer
import json
import asyncio

app = FastAPI(title="Data-Driven Log API")

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "sensor-topic"

producer = None

@app.on_event("startup")
async def startup_event():
    global producer
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()

@app.on_event("shutdown")
async def shutdown_event():
    await producer.stop()

@app.post("/send")
async def send_sensor_data(data: dict):
    try:
        payload = json.dumps(data).encode("utf-8")
        await producer.send_and_wait(KAFKA_TOPIC, payload)
        return {"status": "Success", "message": "Data sent to Kafka", "payload": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
def read_root():
    return {"message": "API is running!"}