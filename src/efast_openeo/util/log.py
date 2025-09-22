import logging

BASE_LOGGER_NAME = "EFAST OpenEO"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(filename)s:%(lineno)d]\t%(message)s"
)

logger = logging.getLogger(BASE_LOGGER_NAME)