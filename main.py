import stackexchange
import time
from pymongo.mongo_client import MongoClient
from delorean import Delorean
from tzlocal import get_localzone
import datetime
from threading import Thread
from stackexchange.core import StackExchangeError
from time import sleep

STACK_EXCHANGE_APP_KEY = '<insert-app-key>'
throttled_for = 0

def get_my_timezone():
    return get_localzone().tzname(datetime.datetime.now())

def insert_questions(db):
    global throttled_for
    so = stackexchange.Site(stackexchange.StackOverflow, app_key=STACK_EXCHANGE_APP_KEY)
    while(True):
        try:
            last_question = db.entries.find({'accepted_answer_text': None}).sort([('question_creation_date', -1)]).limit(1)
            if last_question.count() > 0:
                creation_date_utc = Delorean(last_question[0]['question_creation_date'], 'UTC').shift(get_my_timezone()).datetime.timestamp()
            questions = so.search_advanced(sort="creation", order="asc", accepted=True, fromdate=creation_date_utc)

            i = 0
            for question in questions:
                i = i + 1
                if i%30==0:
                    print(so.rate_limit)
                creation_date_utc = Delorean(question.creation_date, get_my_timezone()).shift('UTC').datetime
                if not db.entries.find_one({'question_id': question.id}):
                    db.entries.insert({
                                       'question_id': question.id,
                                       'question_creation_date': creation_date_utc,
                                       'question_score': question.json['score'],
                                       'tags': question.json['tags'],
                                       'accepted_answer_id': question.json['accepted_answer_id']})
                time.sleep(0.1)
                throttled_for = 0
        except StackExchangeError as e:
            if e.code == 502 and e.name == 'throttle_violation':
                sleep_timeout = int(e.message[59:-8])
                throttled_for = sleep_timeout
                print("[questions] Sleeping from throttling for %d seconds" % sleep_timeout)
                # message = too many requests from this IP, more requests available in %d seconds
                sleep(sleep_timeout)
                continue
            else:
                raise

def insert_answers(db):
    so = stackexchange.Site(stackexchange.StackOverflow, app_key=STACK_EXCHANGE_APP_KEY)
    while(True):
        try:
            if throttled_for > 0:
                print("[answers] Sleeping from throttling for %d seconds" % throttled_for)
                # message = too many requests from this IP, more requests available in %d seconds
                sleep(throttled_for)

            questions = db.entries.find({'accepted_answer_text': None}).sort([('question_creation_date', 1)]).limit(100)
            answer_ids = {}
            if questions.count() < 100:
                print("[answers] Not enough answers to process")
                time.sleep(8)
                continue
            for question in questions:
                answer_ids[question['accepted_answer_id']] = question['_id'] 
            answers = so.answers(list(answer_ids.keys()), body=True)
            for answer in answers:
                db.entries.update(
                                  {'_id': answer_ids[answer.id]},
                                  {'$set': {'accepted_answer_text': answer.json['body'], 'accepted_answer_score': answer.score}})
            print("[answers] Processed batch")
            time.sleep(1)
        except StackExchangeError as e:
            if e.code == 502 and e.name == 'throttle_violation':
                sleep_timeout = int(e.message[59:-8])
                print("[answers] Sleeping from throttling for %d seconds" % sleep_timeout)
                # message = too many requests from this IP, more requests available in %d seconds
                sleep(sleep_timeout)
                continue
            else:
                raise

if __name__ == '__main__':
    db = MongoClient('localhost', 27017)['so']
    inserting_questions = Thread(target=insert_questions, args = [db])
    inserting_questions.start()
    inserting_answers = Thread(target=insert_answers, args = [db])
    inserting_answers.start()