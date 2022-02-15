import typing
from google.cloud import pubsub_v1


def create_subscription(project_id: str, topic_id: str, subscription_id: str) -> None:
    """Create a new pull subscription on the given topic."""
    # [START pubsub_create_pull_subscription]
    from google.cloud import pubsub_v1

    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    # Wrap the subscriber in a 'with' block to automatically call close() to
    # close the underlying gRPC channel when done.
    with subscriber:
        subscription = subscriber.create_subscription(subscription_path,topic_path)

    print(f"Subscription created: {subscription}")


#create_subscription('mlopsmac', 'departed', 'sub_one')
#create_subscription('mlopsmac', 'arrived', 'sub_arrived')

def receive_messages(project_id: str, subscription_id: str, timeout) -> None:
    """Receives messages from a pull subscription."""
    
    from concurrent.futures import TimeoutError
    from google.cloud import pubsub_v1


    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    def callback(message: pubsub_v1.subscriber.message.Message) -> None:
        print(f"Received {message}.")
        message.ack()

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}..\n")

    # Wrap subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        try:
            # When `timeout` is not set, result() will block indefinitely,
            # unless an exception is encountered first.
            streaming_pull_future.result(timeout=timeout)
        except TimeoutError:
            streaming_pull_future.cancel()  # Trigger the shutdown.
            streaming_pull_future.result()  # Block until the shutdown is complete.
    
    
#create_subscription('mlopsmac', 'arrived', 'sub_arrived')   
receive_messages('mlopsmac', 'sub_arrived', 100)