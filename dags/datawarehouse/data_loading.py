import json
from datetime import datetime, date
import logging

logger = logging.getLogger(__name__)

def load_data():
    """Load data from a JSON file named with the current date."""
    file_path = f"./data/video_stats_{date.today()}.json"
    try:
        logger.info(f"Loading data from {file_path}")
        with open(file_path, 'r') as file:
            data = json.load(file)
        logger.info("Data loaded successfully")
        return data
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise
    except json.JSONDecodeError:
        logger.error(f"ERROR: Failed to decode JSON from file: {file_path} ")
        raise
