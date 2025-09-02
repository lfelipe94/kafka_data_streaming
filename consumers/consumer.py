"""Defines core consumer functionality"""
import logging

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

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        
        # Broker properties
        self.broker_properties = {
            "bootstrap.servers": "localhost:9092",
            "group.id": f"{topic_name_pattern}-group",
            # usado si no hay offset almacenado
            "auto.offset.reset": "earliest" if offset_earliest else "latest",
            # evita commits automáticos si luego quieres manejarlos manualmente
            "enable.auto.commit": True,
        }

        # TODO: Create the Consumer, using the appropriate type.
        if is_avro is True:
            self.broker_properties["schema.registry.url"] = "http://localhost:8081"
            self.consumer = AvroConsumer(self.broker_properties)
        else:
            self.consumer = Consumer(self.broker_properties)

        #
        #
        # TODO: Configure the AvroConsumer and subscribe to the topics. Make sure to think about
        # how the `on_assign` callback should be invoked.
        #
        #
        self.consumer.subscribe([self.topic_name_pattern], on_assign=self.on_assign)

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        # TODO: If the topic is configured to use `offset_earliest` set the partition offset to
        # the beginning or earliest
        if self.offset_earliest:
            for p in partitions:
                p.offset = confluent_kafka.OFFSET_BEGINNING
                # también sirve: p.offset = 0
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
        #
        #
        # TODO: Poll Kafka for messages. Make sure to handle any errors or exceptions.
        # Additionally, make sure you return 1 when a message is processed, and 0 when no message
        # is retrieved.
        #
        #
        try:
            msg = self.consumer.poll(self.consume_timeout)
        except Exception as e:
            logger.exception("poll error: %s", e)
            return 0

        if msg is None:
            return 0
        if msg.error():
            logger.error("message error: %s", msg.error())
            return 0

        try:
            logger.debug(
                "Consumed topic=%s partition=%s offset=%s len=%s",
                msg.topic(), msg.partition(), msg.offset(),
                len(msg.value()) if msg.value() else 0,
            )
            self.message_handler(msg)
            return 1
        except SerializerError as e:
            logger.exception("deserialization error: %s", e)
            return 0
        except Exception as e:
            logger.exception("handler error: %s", e)
            return 0


    def close(self):
        """Cleans up any open kafka consumers"""
        #
        #
        # TODO: Cleanup the kafka consumer
        #
        #
        try:
            if hasattr(self, "consumer") and self.consumer is not None:
                self.consumer.close()
        except Exception as e:
            logger.warning("Error closing consumer: %s", e)
