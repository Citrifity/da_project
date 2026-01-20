import os
import time
import random
import json
import logging
import psycopg2
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

dbname = os.environ.get('POSTGRES_DB', 'da_project')
user = os.environ.get('POSTGRES_USER', 'andruha_admin')
password = os.environ.get('POSTGRES_PASSWORD', '123')
host = os.environ.get('DB_HOST', 'db')
img_dir = '/app/images'

conn = None
while conn is None:
    try:
        conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=5432)
        logger.info("Подключение к бд")
    except psycopg2.OperationalError:
        time.sleep(3)

cur = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS image_processing_logs (
        id SERIAL PRIMARY KEY,
        timestamp TIMESTAMP,
        filename VARCHAR(255),
        stage VARCHAR(50),
        status VARCHAR(20),
        processing_time_ms FLOAT,
        confidence_score FLOAT,
        metadata JSONB
    );
""")
conn.commit()

files = [f for f in os.listdir(img_dir) if f.endswith(('.jpg', '.png'))]

stages = ['segmentation', 'bg_removal', 'bg_replacement', 'text_generation']
styles = ['white_studio', 'grey_gradient', 'lifestyle_street']

logger.info("Генерация данных")

while True:
    filename = random.choice(files)
    
    for stage in stages:
        meta = {}
        if stage == 'bg_replacement':
            meta['style'] = random.choice(styles)
        
        timestamp = datetime.now()
        process_time = round(random.uniform(0.1, 1.5), 3)
        confidence = round(random.uniform(0.75, 0.99), 2)
        
        cur.execute("""
            INSERT INTO image_processing_logs 
            (timestamp, filename, stage, status, processing_time_ms, confidence_score, metadata)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (timestamp, filename, stage, 'success', process_time, confidence, json.dumps(meta)))
        
        conn.commit()
        time.sleep(0.5)

    logger.info(f"Обработано изображение {filename}")
    
    time.sleep(random.randint(2, 4))