import time, json, hashlib, logging, os
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

WATCH_DIR = '/mnt/nfs'
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
TOPIC = 'file.received'

def create_producer():
    for i in range(10):
        try:
            p = KafkaProducer(bootstrap_servers=KAFKA_BROKER, value_serializer=lambda v: json.dumps(v).encode())
            logger.info('Conectado a Kafka')
            return p
        except NoBrokersAvailable:
            time.sleep((i+1)*5)

def poll(producer):
    processed = set()
    while True:
        try:
            files = [f for f in os.listdir(WATCH_DIR) if f.endswith('.csv') and os.path.isfile(os.path.join(WATCH_DIR, f))]
            if files:
                logger.info(f'Archivos CSV encontrados: {files}')
            for filename in files:
                if filename in processed:
                    continue
                filepath = os.path.join(WATCH_DIR, filename)
                try:
                    with open(filepath, 'rb') as f:
                        content = f.read()
                    checksum = hashlib.md5(content).hexdigest()
                    event = {
                        'file_id': checksum,
                        'filename': filename,
                        'content': content.decode('utf-8', errors='replace'),
                        'path': filepath,
                        'size': len(content),
                        'received_at': datetime.utcnow().isoformat(),
                        'checksum': checksum,
                    }
                    producer.send(TOPIC, value=event)
                    producer.flush()
                    logger.info(f'Publicado en Kafka: {filename}')
                    os.remove(filepath)
                    processed.add(filename)
                except Exception as fe:
                    logger.error(f'Error leyendo {filename}: {fe}')
        except Exception as e:
            logger.error(f'Error: {e}')
        time.sleep(5)

if __name__ == '__main__':
    logger.info('Watcher iniciado - leyendo desde volumen')
    poll(create_producer())