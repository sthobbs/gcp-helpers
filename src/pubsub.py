import time
from concurrent import futures
from typing import Callable
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound
from google.cloud import pubsub_v1
from src.logger import Logger


class PubSub():
    """Google Cloud Pub/Sub API helper class"""

    def __init__(self, project_id, topic_id, subscription_id=None):
        """
        Initialize the PubSub class.

        Parameters
        ----------
        project_id : str
            Google Cloud project ID (e.g. "my-project")
        topic_id : str
            Google Cloud Pub/Sub topic ID (e.g. "my-topic")
        """

        # pass service account key into credentials
        key_path = './service_account_key.json'
        credentials = service_account.Credentials.from_service_account_file(
            key_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )

        # Configure batch settings for the publisher client.
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

        # Create a Publisher client.
        self.publisher = pubsub_v1.PublisherClient(
            batch_settings=batch_settings,
            publisher_options=pubsub_v1.types.PublisherOptions(
                flow_control=flow_control_settings),
            credentials=credentials
        )

        # Create a Subscriber client.
        self.subscriber = pubsub_v1.SubscriberClient(credentials=credentials)

        # set attributes
        self.project_id = project_id
        self.project_path = f"projects/{project_id}"
        self.topic_path = self.publisher.topic_path(self.project_id, topic_id)
        if subscription_id:
            self.subscription_path = self.subscriber.subscription_path(self.project_id,
                                                                       subscription_id)
        self.publish_exception_count = 0
        self.futures = dict()
        self.received_messages = []
        self.encoding = "utf-8"

    def create_topic(self):
        """Create a new topic."""

        request = pubsub_v1.types.Topic(name=self.topic_path)
        self.publisher.create_topic(request=request)
        self.logger.info(f"Created topic: {self.topic_path}")

    def delete_topic(self):
        """Delete a topic."""

        request = pubsub_v1.types.DeleteTopicRequest(topic=self.topic_path)
        self.publisher.delete_topic(request=request)
        self.logger.info(f"Deleted topic: {self.topic_path}")

    def create_subscription(self, subscription_id=None):
        """
        Create a new subscription.

        Parameters
        ----------
        subscription_id : str
            Google Cloud Pub/Sub subscription ID (e.g. "my-subscription")
        """

        if subscription_id is not None:
            subscription_path = self.subscriber.subscription_path(self.project_id,
                                                                  subscription_id)
        else:
            subscription_path = self.subscription_path
        request = pubsub_v1.types.Subscription(name=subscription_path,
                                               topic=self.topic_path)
        self.subscriber.create_subscription(request=request)
        self.logger.info(f"Created subscription: {subscription_path}")

    def delete_subscription(self, subscription_id=None):
        """
        Delete a subscription.

        Parameters
        ----------
        subscription_id : str
            Google Cloud Pub/Sub subscription ID (e.g. "my-subscription")
        """

        if subscription_id is not None:
            subscription_path = self.subscriber.subscription_path(self.project_id,
                                                                  subscription_id)
        else:
            subscription_path = self.subscription_path
        request = pubsub_v1.types.DeleteSubscriptionRequest(
            subscription=subscription_path)
        self.subscriber.delete_subscription(request=request)
        self.logger.info(f"Deleted subscription: {subscription_path}")

    def list_topics(self):
        """List all topics in the current project."""

        self.logger.info(f"Listing all topics in project {self.project_id}")
        request = pubsub_v1.types.ListTopicsRequest(project=self.project_path)
        page_result = self.publisher.list_topics(request=request)
        for response in page_result:
            print(response)

    def list_topic_subscriptions(self):
        """List all subscriptions for the current topic."""

        self.logger.info(f"Listing all subscriptions in topic {self.topic_path}")
        request = pubsub_v1.types.ListTopicSubscriptionsRequest(topic=self.topic_path)
        try:
            page_result = self.publisher.list_topic_subscriptions(request=request)
        except NotFound:
            msg = f"Topic {self.topic} not found in project {self.project_id}"
            self.logger.warning(msg)
            return
        for response in page_result:
            print(response)

    def list_subscriptions(self):
        """List all subscriptions in the current project."""

        self.logger.info(f"Listing all subscriptions in project {self.project_id}")
        request = pubsub_v1.types.ListSubscriptionsRequest(project=self.project_path)
        page_result = self.subscriber.list_subscriptions(request=request)
        for response in page_result:
            print(response)

    def publish(self, message):
        """
        Publish a message to the current topic.

        Parameters
        ----------
        message : str
            Message to publish
        """

        try:
            if self.encoding:
                message = message.encode(self.encoding)  # messages must be byte strings
            future = self.publisher.publish(self.topic_path, message)
        except Exception as e:
            self.publish_exception_count += 1
            self.logger.warning(f"issue publishing message: {message}, exception: {e}")
        future.result()

    def publish_with_callback(self, message):
        """
        Publish a message to the current topic with a callback.

        Parameters
        ----------
        message : str
            Message to publish
        """

        try:
            if self.encoding:
                message = message.encode(self.encoding)  # messages must be byte strings
            future = self.publisher.publish(self.topic_path, message)
            self.futures[message] = future
        except Exception as e:
            self.publish_exception_count += 1
            self.logger.warning(f"issue publishing message: {message}, exception: {e}")
        future.add_done_callback(self.get_callback(future, message))

    def subscribe(self, timeout=10):
        """
        Subscribe to the current topic.

        Parameters
        ----------
        timeout : int
            Timeout in seconds that the subscriber should listen for messages.
            When time is None, result() will block indefinitely (unless an
            exception is encountered).
        """

        self.logger.info(f"Subscribing to topic: {self.topic_path}")
        future = self.subscriber.subscribe(self.subscription_path,
                                           callback=self.subscriber_callback)

        # Wrap subscriber in a 'with' block to automatically call close() when done.
        with self.subscriber:
            try:
                future.result(timeout=timeout)
            except (KeyboardInterrupt, futures._base.TimeoutError):
                future.cancel()  # Trigger the shutdown.
                future.result()  # Block until the shutdown is complete.
        return self.received_messages

    def subscriber_callback(self, message):
        """
        Callback for subscriber.

        Parameters
        ----------
        message : google.cloud.pubsub_v1.subscriber.message.Message
            Message received from the subscriber
        """

        data = message.data.decode(self.encoding)
        self.logger.info(f"Received message: {data}")
        self.received_messages.append(data)
        message.ack()

    def get_callback(self,
                     future: pubsub_v1.publisher.futures.Future,
                     message: str
                     ) -> Callable[[pubsub_v1.publisher.futures.Future], None]:
        """
        Wrap message data in the context of the callback function.

        Parameters
        ----------
        future : google.cloud.pubsub_v1.publisher.futures.Future
            Future object
        message : str
            Message to publish
        """

        def callback(future: pubsub_v1.publisher.futures.Future) -> None:
            """
            Handle the result of the publish request.

            Parameters
            ----------
            future : google.cloud.pubsub_v1.publisher.futures.Future
                Future object
            """
            try:
                # Wait 60 seconds for the publish call to succeed.
                future.result(timeout=60)
            except futures.TimeoutError:
                self.logger.warning(f"Publishing {message} timed out.")
            except Exception as e:
                self.logger.warning(f"Publishing {message} failed with an exception: {e}")
            self.futures.pop(message)
        return callback

    def wait_for_publish_to_finish(self, min_delay=5, max_delay=30*60):
        """
        Wait for all published messages to be acknowledged by the server.

        Parameters
        ----------
        min_delay : int
            Minimum delay in seconds
        max_delay : int
            Maximum delay in seconds
        """

        self.logger.info("waiting for publishing to finish...")
        time.sleep(min_delay)
        total_delay = min_delay
        while self.futures and total_delay < max_delay:
            time.sleep(5)
            total_delay += 5
