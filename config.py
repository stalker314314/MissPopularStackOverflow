import datetime

from delorean import Delorean


STACK_EXCHANGE_APP_KEY = '<insert-app-key>'
MINIMAL_DATETIME_UTC = Delorean(datetime.datetime(2006, 1, 1), 'UTC').datetime
MAXIMUM_DATETIME_UTC = Delorean(datetime.datetime(2010, 1, 1), 'UTC').datetime

MONGO_SERVER = 'localhost'
MONGO_PORT = 27017