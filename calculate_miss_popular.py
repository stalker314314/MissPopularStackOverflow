import logging.handlers
import time

from pymongo.mongo_client import MongoClient


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.handlers.TimedRotatingFileHandler(filename='calculate_miss_popular.log', when='midnight', interval=1)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

if __name__ == '__main__':
    db = MongoClient('misspopular.cloudapp.net', 26016)['so']

    to_process = db.entries.find({'PostTypeId': 2, 'rawUrlsProcessed': True}).count()
    logger.info("Rows to process: %d", to_process)

    i = 0
    benchmark_start_time = time.time()

    for entry in db.entries.find({'PostTypeId': 2, 'rawUrlsProcessed': True}).sort([('Id', 1)]):
        if i % 10000 == 0:
            logger.info('Processing row %d, speed %f rows/sec', i, 10000 / (time.time() - benchmark_start_time))
            benchmark_start_time = time.time()