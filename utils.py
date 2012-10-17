from werkzeug.wsgi import FileWrapper
from pymongo import ReplicaSetConnection, Connection

import settings


class LimitedFileWrapper(FileWrapper):
    def __init__(self, file, start, end, buffer_size=8192):
        super(LimitedFileWrapper, self).__init__(file, buffer_size=buffer_size)
        self.file.seek(start)
        self._limit = end

    def next(self):
        buffer_size = min(self.buffer_size, self._limit - self.file.tell())
        data = self.file.read(buffer_size)

        if data:
            return data
        raise StopIteration()


def get_mongodb_connection():
    if settings.MONGO_REPLICATION_ON:
        return ReplicaSetConnection(settings.MONGO_REPLICA_SET_URI,
                                    replicaset=settings.MONGO_REPLICA_SET_NAME)
    else:
        return Connection(settings.MONGO_HOST, settings.MONGO_PORT)


class MongoDBConnection(object):
    def __enter__(self):
        self.connection = get_mongodb_connection()
        return self.connection

    def __exit__(self, type, value, traceback):
        self.connection.close()
