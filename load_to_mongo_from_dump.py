import logging.handlers
import time

import dateutil.parser
from pymongo.mongo_client import MongoClient

import xml.etree.ElementTree as etree


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.handlers.TimedRotatingFileHandler(filename='load_to_mongo_from_dump.log', when='midnight', interval=1)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

INTEGER_KEYS = ('Id', 'ParentId', 'LastEditorUserId', 'OwnerUserId', 'PostTypeId', 'ViewCount', 'Score', 'AcceptedAnswerId', 'AnswerCount', 'CommentCount', 'FavoriteCount')
STRING_KEYS = ('Title', 'LastEditorDisplayName', 'Body', 'OwnerDisplayName')
DATE_KEYS = ('CommunityOwnedDate', 'LastActivityDate', 'LastEditDate', 'CreationDate', 'ClosedDate')
LIST_KEYS = ('Tags')

def warning_nonexistant_key(key, value):
    logger.warning('Unknown key %s with value %s', key, value)
    return value

PREPROCESSOR = {
    INTEGER_KEYS: lambda k,v: int(v),
    STRING_KEYS: lambda k,v: v,
    DATE_KEYS: lambda k,v: dateutil.parser.parse(v + 'Z'),
    LIST_KEYS: lambda k,v: v[1:-1].split('><'),
    '': warning_nonexistant_key 
}

if __name__ == '__main__':
    db = MongoClient('localhost', 27017)['so']
    xml = r'c:\users\branko\Desktop\Posts.xml'

    i = 0
    benchmark_start_time = time.time()
    for event, elem in etree.iterparse(xml, events=('end',)):
        if elem.tag != 'row': continue
        entry = dict([key, PREPROCESSOR[next((key_type for key_type in PREPROCESSOR if key in key_type), '')](key, value)] for key,value in elem.attrib.items())
        db.entries.insert(entry)
        elem.clear()
 
        i = i + 1
        if i % 10000 == 0:
            logger.info('Processing row %d, speed %f rows/sec', i, 10000 / (time.time() - benchmark_start_time))
            benchmark_start_time = time.time()