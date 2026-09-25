import time
import logging
import os
from batch_layer.batch_processor import run_batch_reconciliation

logging.basicConfig(
    level=logging.INFO,
    format='{"timestamp": "%(asctime)s", "level": "%(levelname)s", "component": "BatchScheduledRunner", "message": "%(message)s"}'
)
logger = logging.getLogger("BatchScheduledRunner")

INTERVAL_SEC = int(os.getenv("BATCH_INTERVAL_SEC", 30))

def main():
    logger.info(f"Batch Scheduled Runner started. Checking for batch reconciliation every {INTERVAL_SEC}s...")
    # Initial pause to let database and initial files settle
    time.sleep(10)
    while True:
        try:
            run_batch_reconciliation()
        except Exception as e:
            logger.error(f"Error during batch execution: {e}")
        time.sleep(INTERVAL_SEC)

if __name__ == "__main__":
    main()
