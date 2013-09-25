import sys

from pymongo.read_preferences import ReadPreference


# Mongo section
MONGO_HOST = 'localhost'
MONGO_PORT = 27017
MONGO_DB_NAME = 'grid_fs'
MONGO_REPLICATION_ON = True
MONGO_REPLICA_SET_URI = 'localhost:27017,localhost:27018'
MONGO_REPLICA_SET_NAME = 'test_set'
MONGO_READ_PREFERENCE = ReadPreference.SECONDARY_PREFERRED

# App section for builtin server
APP_HOST = '0.0.0.0'
APP_PORT = 5000
DEBUG = True


try:
    from settings_local import *
except ImportError:
    pass


if 'test' in sys.argv[0]: # Is there another way?
    try:
        from settings_test import *
    except ImportError:
        pass
