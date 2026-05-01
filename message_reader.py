import warnings
from google.cloud import pubsub_v1
warnings.filterwarnings("ignore", category=FutureWarning)

# ----------------------------
# CONFIG
# ----------------------------
PROJECT_ID = "project-26fb084d-6d96-4ec6-8a7"
SUBSCRIPTION_ID = "pos-events-sub"

# ----------------------------
# Pull messages
# ----------------------------
def pull_messages():
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        PROJECT_ID, SUBSCRIPTION_ID
    )

    response = subscriber.pull(
        request={
            "subscription": subscription_path,
            "max_messages": 5
        }
    )

    if not response.received_messages:
        print("No messages available.")
        return

    for received_message in response.received_messages:
        message = received_message.message

        print("\n--- MESSAGE RECEIVED ---")
        print(message.data.decode("utf-8"))

        # # Acknowledge so it does not reappear
        # subscriber.acknowledge(
        #     request={
        #         "subscription": subscription_path,
        #         "ack_ids": [received_message.ack_id]
        #     }
        # )

if __name__ == "__main__":
    pull_messages()
