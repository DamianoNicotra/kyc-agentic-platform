import json
import logging
import sys

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger()

def detect_anomaly(event_json):
    try:
        event = json.loads(event_json)
        user_id = event.get('user_id')
        action = event.get('action')
        details = event.get('details', {})
        status = details.get('status') if isinstance(details, dict) else None
        
        if action == 'verification' and status in ['failed', 'rejected']:
            logger.warning(f'ANOMALY DETECTED: user={user_id} action={action} status={status}')
            anomaly_event = {
                'type': 'ANOMALY',
                'user_id': user_id,
                'reason': f'suspicious_{action}_{status}',
                'original_event': event
            }
            return json.dumps(anomaly_event)
        else:
            logger.info(f'Normal event: user={user_id} action={action} status={status}')
            return None
    except Exception as e:
        logger.error(f'Error: {e}')
        return None

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Configura Kafka
    kafka_props = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'flink-consumer',
        'auto.offset.reset': 'earliest'
    }
    
    consumer = FlinkKafkaConsumer(
        topics='kyc-events',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )
    
    stream = env.add_source(consumer)
    stream = stream.map(lambda x: x.strip()).filter(lambda x: x and len(x) > 0)
    anomalies = stream.map(detect_anomaly).filter(lambda x: x is not None)
    anomalies.print()
    
    logger.info('Starting Flink anomaly detector...')
    env.execute('KYC Anomaly Detector')

if __name__ == '__main__':
    main()
