"""Contains functionality related to Weather"""
import logging


logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        #
        #
        # TODO: Process incoming weather messages. Set the temperature and status.
        #
        #
        try:
            val = message.value()  # AvroConsumer ya entrega dict
            if not isinstance(val, dict):
                logger.debug("weather non-dict value=%s", type(val))
                return
            temp = val.get("temperature")
            status = val.get("status")
            if temp is not None:
                self.temperature = float(temp)
            if status:
                # los enums del rest-proxy pueden venir en minúscula
                self.status = str(status).replace("_", " ").title()
            logger.debug("Weather updated: %s° | %s", self.temperature, self.status)
        except Exception as e:
            logger.exception("weather process_message error: %s", e)