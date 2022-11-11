import time
from concurrent import futures
from typing import Callable
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound
from google.cloud import pubsub_v1


class PubSub():

    def __init__(self, project_id, topic_id, subscription_id=None):
        self.project_id = project_id
        self.project_path = f"projects/{project_id}"
        key_path = './service_account_key.json'
        credentials = service_account.Credentials.from_service_account_file(
            key_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        batch_settings = pubsub_v1.types.BatchSettings(
            max_messages=100,  # default 100
            max_bytes=1000000,  # default 1000000 (~1 MB)
            max_latency=1,  # default 0.01 (seconds) (10 ms)
        )
        # Configure how many messages the publisher client can hold in memory
        # and what to do when messages exceed the limit.
        flow_control_settings = pubsub_v1.types.PublishFlowControl(
            message_limit=100,  # 100 messages
            byte_limit=10 * 1024 * 1024,  # 10 MiB
            limit_exceeded_behavior=pubsub_v1.types.LimitExceededBehavior.BLOCK,
        )
        self.publisher = pubsub_v1.PublisherClient(
            batch_settings=batch_settings,
            publisher_options=pubsub_v1.types.PublisherOptions(
                flow_control=flow_control_settings),
            credentials=credentials
        )
        self.subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
        self.topic_path = self.publisher.topic_path(self.project_id, topic_id)
        if subscription_id:
            self.subscription_path = self.subscriber.subscription_path(self.project_id,
                                                                       subscription_id)
        self.publish_exception_count = 0
        self.futures = dict()
        self.received_messages = []
        self.encoding = "utf-8"

    def create_topic(self):
        request = pubsub_v1.types.Topic(name=self.topic_path)
        self.publisher.create_topic(request=request)
        print(f"Created topic: {self.topic_path}")

    def delete_topic(self):
        request = pubsub_v1.types.DeleteTopicRequest(topic=self.topic_path)
        self.publisher.delete_topic(request=request)
        print(f"Deleted topic: {self.topic_path}")

    def create_subscription(self, subscription_id=None):
        if subscription_id is not None:
            subscription_path = self.subscriber.subscription_path(self.project_id,
                                                                  subscription_id)
        else:
            subscription_path = self.subscription_path
        request = pubsub_v1.types.Subscription(name=subscription_path,
                                               topic=self.topic_path)
        response = self.subscriber.create_subscription(request=request)
        print(response)

    def delete_subscription(self, subscription_id=None):
        if subscription_id is not None:
            subscription_path = self.subscriber.subscription_path(self.project_id,
                                                                  subscription_id)
        else:
            subscription_path = self.subscription_path
        request = pubsub_v1.types.DeleteSubscriptionRequest(
            subscription=subscription_path)
        self.subscriber.delete_subscription(request=request)
        print(f"Deleted subscription: {subscription_path}")

    def list_topics(self):
        request = pubsub_v1.types.ListTopicsRequest(project=self.project_path)
        page_result = self.publisher.list_topics(request=request)
        # Handle the response
        for response in page_result:
            print(response)

    def list_topic_subscriptions(self):
        request = pubsub_v1.types.ListTopicSubscriptionsRequest(topic=self.topic_path)
        try:
            page_result = self.publisher.list_topic_subscriptions(request=request)
        except NotFound:
            print("Topic not found")
            return
        # Handle the response
        for response in page_result:
            print(response)

    def list_subscriptions(self):
        request = pubsub_v1.types.ListSubscriptionsRequest(project=self.project_path)
        page_result = self.subscriber.list_subscriptions(request=request)
        # Handle the response
        for response in page_result:
            print(response)

    def publish(self, message, encoding="utf-8"):
        try:
            if self.encoding:
                message = message.encode(self.encoding)
            future = self.publisher.publish(self.topic_path, message)
        except Exception as e:
            self.publish_exception_count += 1
            print(f"Publish in message: {message}, exception: {e}")
        future.result()

    def publish_with_callback(self, message):
        try:
            if self.encoding:
                message = message.encode(self.encoding)
            future = self.publisher.publish(self.topic_path, message)
            self.futures[message] = future
        except Exception as e:
            self.publish_exception_count += 1
            print(f"Publish in message: {message}, exception: {e}")
        future.add_done_callback(self.get_callback(future, message))

    def subscribe(self, timeout=10):
        # Number of seconds the subscriber should listen for messages
        # timeout = 5.0

        future = self.subscriber.subscribe(self.subscription_path,
                                           callback=self.subscriber_callback)
        # Wrap subscriber in a 'with' block to automatically call close() when done.
        with self.subscriber:
            try:
                # When `timeout` is not set, result() will block indefinitely,
                # unless an exception is encountered first.
                future.result(timeout=timeout)
            except (KeyboardInterrupt, futures._base.TimeoutError):
                future.cancel()  # Trigger the shutdown.
                future.result()  # Block until the shutdown is complete.
        return self.received_messages

    def subscriber_callback(self, message):
        data = message.data.decode(self.encoding)
        print(f"Received message: {data}")
        self.received_messages.append(data)
        message.ack()

    def get_callback(self,
                     future: pubsub_v1.publisher.futures.Future,
                     message: str
                     ) -> Callable[[pubsub_v1.publisher.futures.Future], None]:
        def callback(future: pubsub_v1.publisher.futures.Future) -> None:
            try:
                # Wait 60 seconds for the publish call to succeed.
                print(future.result(timeout=60))
            except futures.TimeoutError:
                print(f"Publishing {message} timed out.")
            except Exception as e:
                print(f"Publishing {message} failed with an exception: {e}")
            self.futures.pop(message)
        return callback

    def wait_for_publish_to_finish(self, min_delay=5, max_delay=30*60):
        time.sleep(min_delay)
        total_delay = min_delay
        while self.futures and total_delay < max_delay:
            time.sleep(5)
            total_delay += 5
