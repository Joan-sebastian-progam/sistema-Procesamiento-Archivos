import json, os, logging, csv, io
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
import psycopg2
from psycopg2.extras import RealDictCursor
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
DATABASE_URL = os.getenv('DATABASE_URL')

def get_db():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

def init_db():
    for i in range(10):
        try:
            conn = get_db()
            cur = conn.cursor()
            cur.execute('''CREATE TABLE IF NOT EXISTS file_jobs (
                id VARCHAR PRIMARY KEY, filename VARCHAR NOT NULL,
                status VARCHAR NOT NULL DEFAULT 'received', path VARCHAR,
                size BIGINT, checksum VARCHAR, received_at TIMESTAMPTZ,
                processed_at TIMESTAMPTZ, error_message TEXT,
                rows_processed INTEGER, created_at TIMESTAMPTZ DEFAULT NOW()
            )''')
            conn.commit(); cur.close(); conn.close()
            logger.info('DB inicializada'); return
        except Exception as e:
            logger.warning(f'DB no disponible: {e}'); time.sleep(5)

def update_job(file_id, **kwargs):
    conn = get_db(); cur = conn.cursor()
    fields = ', '.join(f'{k} = %s' for k in kwargs)
    values = list(kwargs.values()) + [file_id]
    cur.execute(f'UPDATE file_jobs SET {fields} WHERE id = %s', values)
    conn.commit(); cur.close(); conn.close()

def create_job(event):
    conn = get_db(); cur = conn.cursor()
    cur.execute('''INSERT INTO file_jobs (id, filename, status, path, size, checksum, received_at)
        VALUES (%s, %s, 'received', %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING''',
        (event['file_id'], event['filename'], event.get('path',''),
         event['size'], event['checksum'], event['received_at']))
    conn.commit(); cur.close(); conn.close()

def validate_content(content_str, filename):
    if not filename.endswith('.csv'):
        raise ValueError(f'Tipo no soportado: {filename}')
    reader = csv.DictReader(io.StringIO(content_str))
    required_cols = {'id', 'name', 'value'}
    if not required_cols.issubset(set(reader.fieldnames or [])):
        raise ValueError(f'Columnas faltantes: {required_cols}')
    rows = list(reader)
    if len(rows) == 0:
        raise ValueError('Archivo vacio')
    return rows

def transform(rows):
    return [{'id': r['id'].strip(), 'name': r['name'].strip().title(),
             'value': float(r['value']), 'processed_at': datetime.utcnow().isoformat()} for r in rows]

def create_producer():
    for i in range(10):
        try:
            p = KafkaProducer(bootstrap_servers=KAFKA_BROKER, value_serializer=lambda v: json.dumps(v).encode())
            logger.info('Conectado a Kafka producer'); return p
        except NoBrokersAvailable:
            time.sleep((i+1)*5)

def process_event(event, producer):
    file_id = event['file_id']
    filename = event['filename']
    content = event.get('content', '')
    logger.info(f'Procesando: {filename}')
    create_job(event)
    update_job(file_id, status='processing')
    try:
        rows = validate_content(content, filename)
        update_job(file_id, status='validated')
        transformed = transform(rows)
        update_job(file_id, status='completed',
                   processed_at=datetime.utcnow().isoformat(),
                   rows_processed=len(transformed))
        producer.send('file.processed', {'file_id': file_id, 'filename': filename, 'rows': len(transformed)})
        logger.info(f'Completado: {filename} ({len(transformed)} filas)')
    except Exception as e:
        logger.error(f'Error procesando {filename}: {e}')
        update_job(file_id, status='failed', error_message=str(e))
        producer.send('file.failed', {'file_id': file_id, 'error': str(e)})

def run():
    init_db()
    producer = create_producer()
    consumer = KafkaConsumer('file.received', bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id='file-processor-group', auto_offset_reset='earliest')
    logger.info('Procesador escuchando en Kafka')
    for message in consumer:
        process_event(message.value, producer)

if __name__ == '__main__':
    run()