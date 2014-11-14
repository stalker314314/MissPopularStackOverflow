import logging.handlers
import time

from pymongo.mongo_client import MongoClient
import pickle
import os

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.handlers.TimedRotatingFileHandler(filename='cache_questions.log', when='midnight', interval=1)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

if __name__ == '__main__':
    db = MongoClient('misspopular.cloudapp.net', 26016)['so']

    questions_cache = {'processed_id': 0, 'questions': {}}
    last_processed_id = 0
    if os.path.exists('questions_cache.p'):
        questions_cache = pickle.load(open('questions_cache.p', 'rb'))
        last_processed_id = questions_cache['processed_id']
        logger.info('Loading pickled cached from id %d, processed entries: %d', last_processed_id, len(questions_cache['questions']))
    else:
        logger.info('Np pickled cache, starting from scratch')

    i = 0
    benchmark_start_time = time.time()

    for entry in db.entries.find({'PostTypeId': 1, 'Id': {'$gt': last_processed_id}}).sort([('Id', 1)]):
        questions_cache['questions'][entry['Id']] = entry['Tags']
        questions_cache['processed_id'] = entry['Id']

        i = i + 1
        if i % 10000 == 0:
            pickle.dump(questions_cache, open('questions_cache.p', 'wb'))
            logger.info('Processing row %d, last processed id %d, speed %f rows/sec', i,  questions_cache['processed_id'], 10000 / (time.time() - benchmark_start_time))
            benchmark_start_time = time.time()
    pickle.dump(questions_cache, open('questions_cache.p', 'wb'))