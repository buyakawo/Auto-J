import os
import logging
from dotenv import load_dotenv

def configure_logging():
    load_dotenv(".env.local")

    level = os.getenv("LOG_LEVEL", "INFO").upper()

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S"
    )