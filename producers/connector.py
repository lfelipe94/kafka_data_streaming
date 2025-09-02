"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging

import requests


logger = logging.getLogger(__name__)


KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "stations"

def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.debug("creating or updating kafka connect connector...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logging.debug("connector already created skipping recreation")
        return

    # TODO: Complete the Kafka Connect Config below.
    # Directions: Use the JDBC Source Connector to connect to Postgres. Load the `stations` table
    # using incrementing mode, with `stop_id` as the incrementing column name.
    # Make sure to think about what an appropriate topic prefix would be, and how frequently Kafka
    # Connect should run this connector (hint: not very often!)
        #return

    # TODO: Complete the Kafka Connect Config below.
    # Directions: Use the JDBC Source Connector to connect to Postgres. Load the `stations` table
    # using incrementing mode, with `stop_id` as the incrementing column name.
    # Make sure to think about what an appropriate topic prefix would be, and how frequently Kafka
    # Connect should run this connector (hint: not very often!)
    # Definición del conector JDBC Source -> Postgres -> Kafka
    payload = {
        "name": CONNECTOR_NAME,
        "config": {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
            "tasks.max": "1",

            # Converters en JSON (sin schemas)
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "false",

            "batch.max.rows": "500",

            # --- Conexión a Postgres (Host URL de tu tabla de servicios) ---
            "connection.url": "jdbc:postgresql://localhost:5432/cta",
            "connection.user": "cta_admin",
            "connection.password": "chicago",

            # --- Qué tabla y cómo leerla ---
            "table.whitelist": "stations",
            "mode": "incrementing",
            "incrementing.column.name": "stop_id",

            # El tópico será <topic.prefix><table_name> => org.chicago.cta.stations
            "topic.prefix": "org.chicago.cta.",

            # No hace falta consultar tan seguido (10 minutos)
            "poll.interval.ms": "600000",

            # Opcional: mapa de tipos para evitar problemas con NUMERIC/DECIMAL si existieran
            # "numeric.mapping": "best_fit"
        },
    }

    # Crear el conector
    resp = requests.post(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload),
    )
    resp.raise_for_status()
    logging.debug("connector created successfully")
    

if __name__ == "__main__":
    configure_connector()
