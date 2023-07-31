import threading
from time import sleep

import more_itertools as mit
import os
import pickle
from collections.abc import MutableMapping

from redis.client import StrictRedis
from redis.connection import ConnectionPool
from redis.sentinel import Sentinel
from sortedcontainers import SortedDict

UUID = 'uuid'
RULE_TO_SET_DICT = {}

sentinels = [
    os.environ['REDIS_SENTINEL_1'],
    os.environ['REDIS_SENTINEL_2']
]
name = os.environ['REDIS_MASTER']
password = os.environ['REDIS_PASS']

sentinel = Sentinel(
    sentinels=[(h, 26379) for h in sentinels],
    socket_timeout=5,
    password=os.environ['REDIS_SENTINEL_PASS'])

host, port = sentinel.discover_master(name)

cp = ConnectionPool(host=host,
                    port=port,
                    password=password)

redis = StrictRedis(
    connection_pool=cp,
    socket_keepalive=True,
    retry_on_timeout=True,
    health_check_interval=10
)


def handle_notification(message, category, container):
    """
    This method processes all the keyspace events and filters out all of them which don't comply with the category. The
    category compliance is the match between the keyspace event code and the corresponding to a particular rule code.
    :param message: a received message.
    :param category: a code used in a Redis dictionary corresponding to a particular rule.
    :param container: a collection which collects all the necessary subset of the changes.
    :return: nothing but an enriched container.
    """
    key = message['channel']
    if bytes('__MDB_REDIS_SLI_CHECK__', 'utf-8') not in key and bytes('__:' + category, 'utf-8') in key:
        key = key.split(b':', 1)[1]
        value = ''
        if category == 's__':
            value = dict(map(lambda kv: (str(kv[0], 'utf-8'), pickle.loads(kv[1])), redis.hgetall(key).items()))
        else:
            for kv in redis.hgetall(key).items():
                value = str(kv[0], 'utf-8')
        key = key.replace(bytes(category, 'utf-8'), bytes(), 1)
        key = pickle.loads(key)
        container[key] = value


def subscribe(category, container):
    """
    Subscribe to keyspace events of the particular rule. The keyspace events represent updates of any particular key
    sourced in one of the compute cluster nodes.
    :param category: represents a corresponding to a particular rule code.
    :param container: a collection where all the changes are saved.
    :return: nothing but a side effect of populating the container.
    """
    pubsub = redis.pubsub()
    pubsub.psubscribe(**{'__keyspace@0__:*': lambda p: handle_notification(p, category, container)})
    pubsub.run_in_thread(sleep_time=0.01)
    check_health(pubsub)


def check_health(pubsub):
    """
    This is a service method whose responsibility is to keep a Redis queue connection alive. It checks health each time
    period starting a new thread.
    :param pubsub: a Redis queue connection.
    :return: nothing but a recursively and infinitely running method.
    """
    t = threading.Timer(5, check_health, [pubsub])
    t.start()
    pubsub.check_health()


class RedisDict(MutableMapping):
    """
    This is a distributed cache wrapper class which provides a bridge between the app data on a single compute node and
    the Redis cluster. The Redis cluster allows to keep several instances of the application in sync.
    """
    def __init__(self, category: str, is_sorted: bool=True):
        self.category = category
        self.outer_changes = dict()
        self._dict = SortedDict() if is_sorted else dict()
        self.read()
        self._changes = set()
        # todo subscribe(category, self.outer_changes)

    def __setitem__(self, key, value):
        self._dict[key] = value
        self._changes.add(key)

    def __delitem__(self, key):
        del self._dict[key]
        self._changes.add(key)

    def __len__(self):
        return len(self._dict)

    def __iter__(self):
        return self._dict.__iter__()

    def __getitem__(self, key):
        return self._dict[key]

    def __repr__(self):
        return self._dict.__repr__()

    def set_dict(self, a):
        self._dict = a
        self._changes = a.keys()

    def get_dict(self):
        return self._dict

    def bisect_left(self, key):
        if not isinstance(self._dict, SortedDict):
            raise NotImplementedError
        return self._dict.bisect_left(key)

    def items(self):
        return self._dict.items()

    def keys(self):
        return self._dict.keys()

    def values(self):
        return self._dict.values()

    def sync(self):
        """
        This method saves all the changes introduced after the last sync from the dictionary to Redis. Thus spreading
        out local-to-this-instance changes.
        :return: nothing but a side effect of flushing data to Redis.
        """
        pipeline = redis.pipeline()
        redis.ping()
        for e in self._changes:
            a = self._dict.get(e, None)
            if not a:
                continue
            if isinstance(a, dict):
                for k, v in a.items():
                    pipeline.hset(bytes(self.category, 'utf-8') + pickle.dumps(e), k, pickle.dumps(v))
                pipeline.hset(bytes(self.category, 'utf-8') + pickle.dumps(e), UUID, pickle.dumps(e))
            if isinstance(a, str):
                pipeline.hset(bytes(self.category, 'utf-8') + pickle.dumps(e), a, '')
        pipeline.execute()
        self.apply_changes()
        self._changes.clear()

    def apply_changes(self):
        """
        There are as many daemon threads as are there instances of the dictionary (one instance per rule). Each thread
        is tuned in to a particular keyspace event type corresponding to the rules.
        This method applies to this instance all the changes received via Redis from all the other instances. Thus
        making incoming changes be applied.
        :return: nothing but a side effect of applying changes.
        """
        count = 0
        while count < 10:
            redis.ping()
            sleep(0.1)
            if not self.outer_changes:
                count += 1
            while self.outer_changes:
                e = self.outer_changes.popitem()
                if e[0] not in self._changes:
                    self._dict[e[0]] = e[1]
                    if self.category == 's__':
                        e[1][UUID] = e[0]

    def read(self):
        """
        Reads formerly saved data from Redis.
        :return: nothing but a side effect of populating all the data structures in the dictionary.
        """
        pipeline = redis.pipeline()
        for a in mit.chunked(redis.keys(self.category + '*'), 100000):
            for b in a:
                pipeline.hgetall(b)
            for h in pipeline.execute():
                if self.category == 's__':
                    value = dict(map(lambda kv: (str(kv[0], 'utf-8'), pickle.loads(kv[1])), h.items()))
                    self._dict[value[UUID]] = value
                else:
                    for kv in h.items():
                        value = str(kv[0], 'utf-8')
                        self._dict[value] = ''
