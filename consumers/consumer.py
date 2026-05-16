"""Defines core consumer functionality"""
import logging
import json

import confluent_kafka
from confluent_kafka import Consumer
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


logger = logging.getLogger(__name__)


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest
        self.is_avro = is_avro

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {
                #
                # TODO
                #
            "bootstrap.servers": "localhost:9092",
            "group.id": f"{topic_name_pattern}-consumer",
            "default.topic.config": {
                "auto.offset.reset": "earliest" if offset_earliest else "latest"
            },
        }

        # TODO: Create the Consumer, using the appropriate type.
        if is_avro is True:
            self.broker_properties["schema.registry.url"] = "http://localhost:8081"
            self.consumer = AvroConsumer(self.broker_properties)
        else:
            self.consumer = Consumer(self.broker_properties)

        # Subscribe using regex pattern
        self.consumer.subscribe(
            [self.topic_name_pattern],
            on_assign=self.on_assign,
        )

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        # TODO: If the topic is configured to use `offset_earliest` set the partition offset to
        # the beginning or earliest
        if self.offset_earliest:
            for partition in partitions:
                partition.offset = confluent_kafka.OFFSET_BEGINNING
                #
                #
                # TODO
                #
                #

        logger.info("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        try:
            message = self.consumer.poll(self.consume_timeout)

            if message is None:
                return 0

            if message.error():
                logger.error("consumer error: %s", message.error())
                return 0

            # Normal Kafka Consumer returns bytes for JSON topics.
            # AvroConsumer already returns decoded dicts.
            if not self.is_avro:
                raw_value = message.value()

                if raw_value is None:
                    return 0

                try:
                    decoded_value = json.loads(raw_value.decode("utf-8"))
                except Exception as e:
                    logger.exception("failed to decode JSON message: %s", e)
                    return 0

                # Wrap decoded value so existing handlers can still call message.value()
                class DecodedMessage:
                    def __init__(self, original_message, value):
                        self.original_message = original_message
                        self._value = value

                    def value(self):
                        return self._value

                    def key(self):
                        return self.original_message.key()

                    def topic(self):
                        return self.original_message.topic()

                    def partition(self):
                        return self.original_message.partition()

                    def offset(self):
                        return self.original_message.offset()

                message = DecodedMessage(message, decoded_value)

            self.message_handler(message)
            return 1

        except SerializerError as e:
            logger.exception("message deserialization failed: %s", e)
            return 0

        except Exception as e:
            logger.exception("failed to consume message: %s", e)
            return 0


    def close(self):
        """Cleans up any open kafka consumers"""
        #
        #
        # TODO: Cleanup the kafka consumer
        #
        #
        try:
            self.consumer.close()
            logger.info("closed consumer for %s", self.topic_name_pattern)
        except Exception as e:
            logger.exception("failed to close consumer: %s", e)
