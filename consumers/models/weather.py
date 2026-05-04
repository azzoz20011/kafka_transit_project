"""Contains functionality related to Weather"""
import logging
import json


logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        #logger.info("weather process_message is incomplete - skipping")
        #
        #
        # TODO: Process incoming weather messages. Set the temperature and status.
        #
        #
        try:
            value = message.value()

            if isinstance(value, dict):
                json_data = value
            else:
                if isinstance(value, bytes):
                    value = value.decode("utf-8")

                json_data = json.loads(value)

            self.temperature = json_data.get("temperature", self.temperature)
            self.status = json_data.get("status", self.status)

            logger.debug(
                "updated weather: temperature=%s status=%s",
                self.temperature,
                self.status,
            )

        except Exception as e:
            logger.exception("failed to process weather message: %s", e)
