import datetime
import logging.handlers
from os.path import sys
from threading import Thread
from time import sleep
import time
import traceback

from delorean import Delorean
from pymongo.mongo_client import MongoClient
import stackexchange
from stackexchange.core import StackExchangeError
from tzlocal import get_localzone
import config

throttled_for = 0

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.handlers.TimedRotatingFileHandler(filename='misspopular.log', when='midnight', interval=1)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

#ch = logging.StreamHandler()
#ch.setLevel(logging.DEBUG)
#logger.addHandler(ch)

def exception_hook(exctype, value, tb):
    logger.critical('{0}: {1}'.format(exctype, value))
    logger.critical(''.join(traceback.format_tb(tb)))

def get_my_timezone():
    return get_localzone().tzname(datetime.datetime.now())

def insert_questions(db):
    global throttled_for
    so = stackexchange.Site(stackexchange.StackOverflow, app_key=config.STACK_EXCHANGE_APP_KEY)
    while(True):
        try:
            creation_date_utc = config.MINIMAL_DATETIME_UTC.timestamp()
            last_question = db.entries \
                .find({'question_creation_date': {'$lt': config.MAXIMUM_DATETIME_UTC, '$gte': config.MINIMAL_DATETIME_UTC}}) \
                .sort([('question_creation_date', -1)]) \
                .limit(1)
            if last_question.count() > 0:
                last_datetime_utc = Delorean(last_question[0]['question_creation_date'], 'UTC').shift(get_my_timezone()).datetime.timestamp()
                if last_datetime_utc > creation_date_utc:
                    creation_date_utc = last_datetime_utc
            questions = so.search_advanced(sort='creation', order='asc', accepted=True, fromdate=creation_date_utc)

            i = 0
            for question in questions:
                i = i + 1
                if i % 30 == 0:
                    logger.info('[questions] Requests left: %d/%d', so.rate_limit[0], so.rate_limit[1])
                creation_date_utc = Delorean(question.creation_date, get_my_timezone()).shift('UTC').datetime
                if not db.entries.find_one({'question_id': question.id}):
                    logger.info('[questions] Inserting question %d from time %s', question.id, creation_date_utc)
                    db.entries.insert({
                                       'question_id': question.id,
                                       'question_creation_date': creation_date_utc,
                                       'question_score': question.json['score'],
                                       'tags': question.json['tags'],
                                       'accepted_answer_id': question.json['accepted_answer_id']})
                else:
                    logger.info('Question %d from time %s already exists', question.id, creation_date_utc)
                time.sleep(0.1)
                throttled_for = 0
        except StackExchangeError as e:
            if e.code == 502 and e.name == 'throttle_violation':
                sleep_timeout = int(e.message[59:-8])
                throttled_for = sleep_timeout
                logger.info('[questions] Sleeping from throttling for %d seconds', sleep_timeout)
                # message = too many requests from this IP, more requests available in %d seconds
                sleep(sleep_timeout)
                continue
            else:
                raise

def insert_answers(db):
    so = stackexchange.Site(stackexchange.StackOverflow, app_key=config.STACK_EXCHANGE_APP_KEY)
    while(True):
        try:
            if throttled_for > 0:
                logger.info('[answers] Sleeping from throttling for %d seconds', throttled_for)
                # message = too many requests from this IP, more requests available in %d seconds
                sleep(throttled_for)

            questions = db.entries \
                .find(
                      {
                       'accepted_answer_text': None,
                       'question_creation_date': {'$lt': config.MAXIMUM_DATETIME_UTC, '$gte': config.MINIMAL_DATETIME_UTC}}) \
                .sort([('question_creation_date', 1)]) \
                .limit(30)
            answer_ids = {}
            if questions.count() < 30:
                logger.info('[answers] Not enough answers to process')
                time.sleep(10)
                continue
            for question in questions:
                answer_ids[question['accepted_answer_id']] = question['_id'] 
            answers = so.answers(list(answer_ids.keys()), body=True, sort='creation', order='asc')
            processed = 0
            for answer in answers:
                if answer.id in answer_ids:
                    db.entries.update(
                                      {'_id': answer_ids[answer.id]},
                                      {'$set': {'accepted_answer_text': answer.json['body'], 'accepted_answer_score': answer.score}})
                    processed = processed + 1
                    del answer_ids[answer.id]
            logger.info('[answers] Processed batch of size %d', processed)
            if len(answer_ids) > 0:
                logger.warning('[answers] Unprocessed answers (%d) which answers will be removed: %s', len(answer_ids), str(answer_ids))
                for answer_id in answer_ids.keys():
                    db.entries.remove({'accepted_answer_id': answer_id})
            time.sleep(1)
        except StackExchangeError as e:
            if e.code == 502 and e.name == 'throttle_violation':
                # message = too many requests from this IP, more requests available in %d seconds
                sleep_timeout = int(e.message[59:-8])
                logger.info('[answers] Sleeping from throttling for %d seconds', sleep_timeout)
                sleep(sleep_timeout)
                continue
            else:
                raise

if __name__ == '__main__':
    sys.excepthook = exception_hook
    db = MongoClient(config.MONGO_SERVER, config.MONGO_PORT)['so']
    inserting_questions = Thread(target=insert_questions, args = [db])
    inserting_questions.start()
    inserting_answers = Thread(target=insert_answers, args = [db])
    inserting_answers.start()