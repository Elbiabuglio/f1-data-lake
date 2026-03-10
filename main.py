import datetime
import os
import time

import dotenv
import logging
from collect import CollectResults
from sender import Sender

dotenv.load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

BUCKET_NAME = os.getenv("BUCKET_NAME")

while True:

    logger.info("Iniciando processo...")

    logger.info("Coletando dados...")
    collect_data = CollectResults(year=[datetime.datetime.now().year])
    collect_data.process_years()

    logger.info("Enviando dados...")
    sender_data = Sender(bucket_name=BUCKET_NAME, bucket_folder="f1/results")
    sender_data.process_folder("data/*")

    logger.info("Iteracao finalizada.")
    time.sleep(60 * 60 * 6)