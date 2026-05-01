import warnings
import time
warnings.filterwarnings("ignore", category=FutureWarning)

import json
import uuid
from datetime import datetime
from google.cloud import pubsub_v1
from stream_generator import generate_pos_event

PROJECT_ID = "project-26fb084d-6d96-4ec6-8a7"
TOPIC_ID = "pos-events"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

def publish_event(request=None):
    event = generate_pos_event()
    future = publisher.publish(
        topic_path,
        json.dumps(event).encode("utf-8")
    )
    message_id = future.result()
    print(f"Published message ID: {message_id}")

if __name__ == "__main__":
    while True:
        time.sleep(1)
        publish_event()
