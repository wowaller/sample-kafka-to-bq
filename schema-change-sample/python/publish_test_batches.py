import time
import json
import logging
from google.cloud import pubsub_v1

PROJECT_ID = 'binggang-lab'
TOPIC_ID = 'schema-update-test-topic'
TOPIC_PATH = f"projects/{PROJECT_ID}/topics/{TOPIC_ID}"

logging.basicConfig(level=logging.INFO)
publisher = pubsub_v1.PublisherClient()

def publish_batch(batch_name, start_idx, count, additional_fields=None):
    logging.info(f"Publishing {count} messages for {batch_name}...")
    for i in range(start_idx, start_idx + count):
        payload = {
            'ID': f"order_{batch_name}_{i}",
            'SENDER_NAME': f"Sender {batch_name} {i}"
        }
        if additional_fields:
            payload.update(additional_fields)
            
        data = json.dumps(payload).encode('utf-8')
        future = publisher.publish(TOPIC_PATH, data)
        # We can wait for the future if we want to ensure ordering/completion, or let it run in parallel.
        # Let's wait to ensure we don't overwhelm local sockets, but it's very fast.
        future.result()
        
    logging.info(f"Completed {batch_name}.")

if __name__ == '__main__':
    # Batch 1: 100 messages with base schema
    publish_batch("batch1", 1, 100)
    
    logging.info("Sleeping 30 seconds to let Batch 1 settle...")
    time.sleep(30)
    
    # Batch 2: 100 messages with FIELD_BATCH_1
    publish_batch("batch2", 1, 100, {'FIELD_BATCH_1': 'batch2_value'})
    
    logging.info("Sleeping 30 seconds to let Batch 2 settle and evolve schema...")
    time.sleep(30)
    
    # Batch 3: 100 messages with FIELD_BATCH_2
    publish_batch("batch3", 1, 100, {'FIELD_BATCH_1': 'batch3_value', 'FIELD_BATCH_2': 'batch3_value'})
    
    logging.info("All batches published successfully!")
