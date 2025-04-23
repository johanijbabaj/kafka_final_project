# Configures logging settings for the application.
# Sets the log level based on the LOG_LEVEL environment variable, defaulting to INFO. 
# The log format includes timestamp, log level, logger name, and message. 
# The logging level for the "kafka" logger is set to WARNING to reduce log output from Kafka.

import logging
import os


def configure_logging():
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/app.log", mode="a", encoding="utf-8"),
    ],
    )

    logging.getLogger("kafka").setLevel(logging.INFO)  # Reduce logging level for Kafka