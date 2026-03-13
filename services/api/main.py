from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import json
import asyncio
from kafka import KafkaConsumer
import threading
from typing import List
from datetime import datetime

app = FastAPI(title="File Processor API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DATABASE_URL = os.getenv("DATABASE_URL")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

class ConnectionManager:
    def __init__(self):
        self.active: List[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        self.active.remove(ws)

    async def broadcast(self, data: dict):
        for ws in self.active.copy():
            try:
                await ws.send_json(data)
            except:
                self.active.remove(ws)

manager = ConnectionManager()


def kafka_listener():
    consumer = KafkaConsumer(
        "file.processed", "file.failed",
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id="api-broadcaster",
    )
    loop = asyncio.new_event_loop()
    for msg in consumer:
        event = {"topic": msg.topic, "data": msg.value}
        loop.run_until_complete(manager.broadcast(event))

threading.Thread(target=kafka_listener, daemon=True).start()


def get_db():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

@app.get("/jobs")
def list_jobs(status: str = None, limit: int = 50):
    conn = get_db()
    cur = conn.cursor()
    if status:
        cur.execute("SELECT * FROM file_jobs WHERE status = %s ORDER BY created_at DESC LIMIT %s", (status, limit))
    else:
        cur.execute("SELECT * FROM file_jobs ORDER BY created_at DESC LIMIT %s", (limit,))
    jobs = cur.fetchall()
    cur.close()
    conn.close()
    return {"jobs": [dict(j) for j in jobs]}

@app.get("/jobs/{file_id}")
def get_job(file_id: str):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM file_jobs WHERE id = %s", (file_id,))
    job = cur.fetchone()
    cur.close()
    conn.close()
    return dict(job) if job else {"error": "not found"}

@app.get("/stats")
def get_stats():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT 
            status,
            COUNT(*) as count,
            SUM(size) as total_size,
            SUM(rows_processed) as total_rows
        FROM file_jobs
        GROUP BY status
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {"stats": [dict(r) for r in rows]}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()  
    except WebSocketDisconnect:
        manager.disconnect(websocket)