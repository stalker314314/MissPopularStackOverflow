#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging.handlers
import re
import time

from pymongo.mongo_client import MongoClient
from threading import Thread


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.handlers.TimedRotatingFileHandler(filename='insert_urls.log', when='midnight', interval=1)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

def process_shard(minimum_id, maximum_id):
    db = MongoClient('misspopular.cloudapp.net', 26016)['so']
    
    #to_process = db.entries.find({'PostTypeId': 2, 'rawUrlsProcessed': None, 'Id': {'$gte': minimum_id, '$lte': maximum_id}}).count()
    #logger.info("Rows to process: %d", to_process)
    
    i = 0
    benchmark_start_time = time.time()
    # kindly taken from https://gist.github.com/gruber/8891611
    expression=r'(?i)\b((?:https?:(?:/{1,3}|[a-z0-9%])|[a-z0-9.\-]+[.](?:com|net|org|edu|gov|mil|aero|asia|biz|cat|coop|info|int|jobs|mobi|museum|name|post|pro|tel|travel|xxx|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cs|cu|cv|cx|cy|cz|dd|de|dj|dk|dm|do|dz|ec|ee|eg|eh|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|Ja|sk|sl|sm|sn|so|sr|ss|st|su|sv|sx|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw)/)(?:[^\s()<>{}\[\]]+|\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\))+(?:\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\)|[^\s`!()\[\]{};:\'".,<>?Â«Â»â€œâ€â€˜â€™])|(?:(?<!@)[a-z0-9]+(?:[.\-][a-z0-9]+)*[.](?:com|net|org|edu|gov|mil|aero|asia|biz|cat|coop|info|int|jobs|mobi|museum|name|post|pro|tel|travel|xxx|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cs|cu|cv|cx|cy|cz|dd|de|dj|dk|dm|do|dz|ec|ee|eg|eh|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|Ja|sk|sl|sm|sn|so|sr|ss|st|su|sv|sx|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw)\b/?(?!@)))'
    compiled_re = re.compile(expression)

    for entry in db.entries.find({'PostTypeId': 2, 'rawUrlsProcessed': None, 'Id': {'$gte': minimum_id, '$lte': maximum_id}}).sort([('Id', 1)]):
        whole_list = set(re.findall(compiled_re, entry['Body']))
        whole_list = [x for x in whole_list if x.startswith('http://') or x.startswith('https://')]
        #if len(whole_list) > 0:
        #    logger.info("%d-%d - %s", minimum_id, maximum_id, whole_list)
        db.entries.update(
                  {'_id': entry['_id']},
                  {'$set': {'rawUrls': whole_list, 'rawUrlsProcessed':True}})
        i = i + 1
        if i % 1000 == 0:
            logger.info('%d-%d: Processing row %d, speed %f rows/sec', minimum_id, maximum_id, i, 1000 / (time.time() - benchmark_start_time))
            benchmark_start_time = time.time()
    logger.info("Finished thread %d-%d", minimum_id, maximum_id)

if __name__ == '__main__':
    inserting_answers = []
    for i in range(0, 10):
        logger.info("%d", i)
        t = Thread(target=process_shard, args = [i*100000, (i+1) * 100000])
        inserting_answers.append(t)
        t.start()
    for t in inserting_answers:
        t.join()
    #to_process = db.entries.find({'PostTypeId': 2, 'rawUrlsProcessed': None}).count()
    #logger.info("Rows to process: %d", to_process)