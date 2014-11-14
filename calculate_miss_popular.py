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

# ('tag', 'another tag') => {'url': count}
POPULARS = {}
if __name__ == '__main__':
    db = MongoClient('misspopular.cloudapp.net', 26016)['so']

    i = 0
    benchmark_start_time = time.time()

    questions_cache = {}
    for entry in db.entries.find({'PostTypeId': 1}):
        questions_cache[entry['Id']] = entry['Tags']
        
        i = i + 1
        if i % 10000 == 0:
            logger.info('Processing row %d, speed %f rows/sec', i, 10000 / (time.time() - benchmark_start_time))
            benchmark_start_time = time.time()

    i = 0
    for entry in db.entries.find({'PostTypeId': 2, 'rawUrlsProcessed': True, 'Id': {'$lte': 10000}, 'rawUrls': {'$not': {'$size': 0}}}).sort([('Id', 1)]):
        parentId = entry['ParentId']
        parent = db.entries.find_one({'Id': parentId})
        if parent['PostTypeId'] != 1:
            logger.info('Not expecting this for id: %d', entry['Id'])
        for tag in parent['Tags']:
            if (tag,) not in POPULARS:
                POPULARS[(tag,)] = {}
            for url in entry['rawUrls']:
                if url not in POPULARS[(tag,)]:
                    POPULARS[(tag,)][url] = 0
                POPULARS[(tag,)][url] += 1 
        i = i + 1
        if i % 100 == 0:
            logger.info('Length: %d', len(POPULARS))
            logger.info('%s', POPULARS)
            logger.info('Processing row %d, speed %f rows/sec', i, 100 / (time.time() - benchmark_start_time))
            benchmark_start_time = time.time()