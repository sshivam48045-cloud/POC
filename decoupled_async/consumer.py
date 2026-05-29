from confluent_kafka import Consumer, KafkaError
import redis
import json
import os

r = redis.Redis(host='localhost', port=6379, db=0)

conf = {
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'crustdata_curation_group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(conf)
consumer.subscribe(['crustdata-burst-stream'])

print("🔥 AI Agent Consumer Started. Waiting for data...")

while True:
    msg = consumer.poll(1.0)
    if msg is None: continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF: continue
        else: print(msg.error()); break

    data = json.loads(msg.value().decode('utf-8'))
    task_id = data['task_id']
    
    # 1. DEDUPLICATION LAYER
    if r.sismember("processed_tasks", task_id):
        print(f"Duplicate caught! Dropping task: {task_id}")
        continue
    r.sadd("processed_tasks", task_id)

    # 2. SCORING ALGORITHM (Data Curation)
    # trust * 0.4 + freshness * 0.3 + popularity * 0.2 + content * 0.1
    score = (data['trust'] * 0.4) + (data['freshness'] * 0.3) + (data['popularity'] * 0.2) + (data['content_score'] * 0.1)
    
    # 3. STORAGE ROUTING BASED ON SCORE
    if score >= 80:
        # HIGH SCORE -> RAM (Redis Hash)
        r.hset("mock_hnsw_ram", task_id, json.dumps(data))
        print(f"🚀 Score {score}: Saved to HIGH-SPEED RAM (Redis)")
    
    elif 50 <= score < 80:
        # MID SCORE -> WARM STORAGE (Local JSON File)
        with open("warm_storage.json", "a") as f:
            f.write(json.dumps(data) + "\n")
        print(f"📦 Score {score}: Saved to WARM STORAGE (File DB)")
    
    else:
        # LOWEST SCORE -> COLD STORAGE (CSV/Log dump)
        with open("cold_storage.csv", "a") as f:
            f.write(f"{task_id},{score}\n")
        print(f"❄️ Score {score}: Saved to COLD STORAGE (Archive)")
