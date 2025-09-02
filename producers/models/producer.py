"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {
            "bootstrap.servers": "localhost:9092",
            "schema.registry.url": "http://localhost:8081",
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # TODO: Configure the AvroProducer
        self.producer = AvroProducer(            
            {
                "bootstrap.servers": self.broker_properties["bootstrap.servers"],
                "schema.registry.url": self.broker_properties["schema.registry.url"],
            },
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema,
        )

    # -------- utilidades de logging --------
    def _delivery_report(self, err, msg):
        """Callback de confirmación de envío"""
        if err is not None:
            logger.error(f"[DELIVERY-ERROR] topic={msg.topic()} => {err}")
        else:
            logger.debug(
                f"[DELIVERED] topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}"
            )

    def produce(self, topic=None, key=None, value=None):
        """Envoltura con logging para producir eventos"""
        topic = topic or self.topic_name
        logger.info(f"[PRODUCE] topic={topic} key={key} value={value}")
        # Nota: pasamos el callback para ver confirmación
        self.producer.produce(topic=topic, key=key, value=value, on_delivery=self._delivery_report)

    # ---------------------------------------

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        try:
            admin = AdminClient({"bootstrap.servers": self.broker_properties["bootstrap.servers"]})

            # Avoid to recreat if it already exists
            metadata = admin.list_topics(timeout=5)
            if self.topic_name in metadata.topics:
                logger.info(f"Topic '{self.topic_name}' already exists")
                return

            topic = NewTopic(
                topic=self.topic_name,
                num_partitions=self.num_partitions,
                replication_factor=self.num_replicas,
            )

            futures = admin.create_topics([topic])
            fut = futures[self.topic_name]
            fut.result()  # Raises on error
        except Exception as e:
            logger.warning(f"Topic creation for '{self.topic_name}' skipped/failed: {e}")
        
        
    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        try:
            if hasattr(self, "producer") and self.producer is not None:
                self.producer.flush()  # vacía el buffer y envía mensajes pendientes
        except Exception as e:
            logger.warning(f"Error flushing producer: {e}")
        finally:
            logger.info("producer close incomplete - skipping")


    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
