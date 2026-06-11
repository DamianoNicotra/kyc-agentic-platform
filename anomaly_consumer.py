import json
from kafka import KafkaConsumer

# Configurazione
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'kyc-events'
GROUP_ID = 'anomaly-detector'

# Statistiche per utente
failure_counts = {}

print(f"Starting anomaly consumer on {TOPIC}...")
print("=" * 60)

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    group_id=GROUP_ID,
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for msg in consumer:
    event = msg.value
    user_id = event.get('user_id')
    action = event.get('action')
    details = event.get('details', {})
    
    # Controlla anomalie
    is_anomaly = False
    anomaly_type = None
    
    # Regola 1: troppi fallimenti
    if action == 'verification' and details.get('status') == 'failed':
        if user_id not in failure_counts:
            failure_counts[user_id] = 0
        failure_counts[user_id] += 1
        
        if failure_counts[user_id] >= 3:
            is_anomaly = True
            anomaly_type = 'TOO_MANY_FAILURES'
            print(f"⚠️ ANOMALY: User {user_id} failed {failure_counts[user_id]} times!")
    
    # Regola 2: upload documento e immediata approvazione (sospetto)
    elif action == 'doc_upload':
        print(f"📄 Document upload from {user_id}")
    
    # Regola 3: operazioni notturne (tra 2am e 5am)
    elif action == 'verification' and details.get('status') == 'approved':
        import datetime
        hour = datetime.datetime.now().hour
        if 2 <= hour <= 5:
            is_anomaly = True
            anomaly_type = 'NIGHT_OPERATION'
            print(f"🌙 ANOMALY: User {user_id} verified at {hour}:00 (night time)")
    
    if is_anomaly:
        print(f"   → Reason: {anomaly_type}")
        print("=" * 60)
    else:
        print(f"✓ Normal: {user_id} → {action} ({details.get('status', 'ok')})")
