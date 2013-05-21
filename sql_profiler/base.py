from collections import defaultdict
import copy
from datetime import datetime
from datetime import timedelta
from functools import wraps
import os
import pickle
import traceback

from django.db.backends.postgresql_psycopg2 import base
from django.conf import settings

import redis

redis_connection_pool = redis.ConnectionPool(
    host=getattr(settings, 'REDIS_HOST', 'localhost'),
    port=getattr(settings, 'REDIS_PORT', 6379),
    db=getattr(settings, 'REDIS_PROFILE_DB', 0),
)

def get_storage(version=os.environ['APP_VERSION']):
    connection = redis.Redis(connection_pool=redis_connection_pool)
    storage = RedisStorage(connection, version)
    storage.add_version(version)
    return storage


class DatabaseWrapper(base.DatabaseWrapper):
    def _cursor(self):
        cursor = super(DatabaseWrapper, self)._cursor()
        cursor.__class__ = CursorWrapper
        cursor.profile_storage = get_storage()
        return cursor


class CursorWrapper(base.CursorWrapper):
    def execute(self, query, args=None):
        return self._measure(super(CursorWrapper, self).execute)(query, args)

    def executemany(self, query, args):
        return self._measure(super(CursorWrapper, self).executemany(query, args))

    def _measure(self, function):
        @wraps(function)
        def wrapper(query, args):
            start = datetime.now()
            result = function(query, args)
            end = datetime.now()
            self.profile_storage.store_call(traceback.extract_stack(), end - start)
            return result
        return wrapper


class RedisStorage(object):
    def __init__(self, connection, version):
        self.connection = connection
        self.count_prefix = "profile_%s_count_" % version
        self.time_prefix = "profile_%s_time_" % version

    def store_call(self, stacktrace, time):
        stacktrace = pickle.dumps(tuple(stacktrace)[:-1])
        self.connection.incr("%s%s" % (self.count_prefix, stacktrace))
        self.connection.incrbyfloat("%s%s" % (self.time_prefix, stacktrace), time.total_seconds())

    def get_count(self):
        return self._get_set(self.count_prefix, int)

    def get_time(self):
        return self._get_set(self.time_prefix, float)

    def _get_set(self, prefix, value_type):
        keys_raw = self.connection.keys('%s*' % prefix)
        values = self.connection.mget(keys_raw)
        keys = [pickle.loads(key[len(prefix):]) for key in keys_raw]
        return dict(zip(keys, map(value_type, values)))

    def add_version(self, version):
        self.connection.sadd('profile_versions', version)

    def get_versions(self):
        return self.connection.smembers('profile_versions')
