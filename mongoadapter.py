import datetime
import json
import os

import pandas as pd
from bson import json_util, ObjectId
from pymongo import MongoClient
from bson.codec_options import DatetimeConversion
import hashlib
import phonenumbers
from pandas import json_normalize

UUID = 'uuid'

mongo_db = 'person'
mongo_collection = 'snapshot'
mongo_host = SOME_HOST
mongo_port = 27018
mongo_user = os.environ['MONGO_USER']
mongo_pass = os.environ['MONGO_PASS']

mongo_auth_db_local = 'admin'
mongo_db_local = 'person'
mongo_collection_local = 'snapshot'
mongo_host_local = 'localhost'
mongo_port_local = 27017
mongo_user_local = 'root'
mongo_pass_local = 'example'

TOP_LEVEL_KEY = 'top'
TOP_LEVEL_HASH = int.from_bytes(hashlib.sha256(TOP_LEVEL_KEY.encode('utf-8')).digest(), 'big')
KEY_SUFFIX = '00'


def read_data(skip=0, limit=1000000, json_data={}, file_name=None):
    """
    This method reads data from the database and dumps it to a file.

    :param skip: a number of entities from the database to skip before reading the data.
    :param limit: a number of records from the database which needs to be read.
    :param json_data: a container where to write data to.
    :param file_name: a file name where to dump JSON data.
    :return: a side effect of populating a file with data.
    """
    cursor = prepare_cursor(skip, limit)

    hash_to_key = dict()
    counter = 1
    for entity in cursor:
        data = dict()
        flatten_dict_simple(entity, data, TOP_LEVEL_KEY)
        json_data[counter] = data
        counter += 1
    file_name = str(skip) + '_python.json'
    if file_name:
        write_to_files(file_name, json_data, hash_to_key)


def read_data_to_df(skip=0, limit=1000000, is_no_id=True):
    """
    This method reads data from the database and returns a pandas dataframe.

    :param skip: a number of entities from the database to skip before reading the data.
    :param limit: a number of records from the database which needs to be read.
    :param is_no_id: if there's a need to delete an id column from the dataframe.

    :return: a pandas dataframe.
    """

    cursor = prepare_cursor(skip, limit)
    df =  pd.DataFrame(json_normalize(list(cursor)))

    if is_no_id:
        del df['_id']

    return df


def prepare_cursor(skip=0, limit=1000000):
    projection = {
        'createdAt': False,
        'updatedAt': False,
        'person.documents.russianPassport.issueDate': False
    }
    query = {}
    cursor = read_mongo(
        auth_db=mongo_db,
        db=mongo_db,
        collection=mongo_collection,
        query=query,
        host=mongo_host,
        port=mongo_port,
        username=mongo_user,
        password=mongo_pass,
        projection=projection,
        skip=skip,
        limit=int(limit)
    )
    return cursor

def write_to_files(file_name, result, hash_to_key):
    with open(file_name.split('.')[0] + '.json', 'a', encoding='utf-8') as f:
        f.write(json_util.dumps(result, indent=4))
    with open(file_name.split('.')[0] + '_hash.json', 'a', encoding='utf-8') as f:
        f.write(json_util.dumps(hash_to_key, indent=4))


def _connect_mongo(host, port, username, password, auth_db, db):
    if username and password:
        mongo_uri = 'mongodb://%s:%s@%s:%s/%s' % (username, password, host, port, auth_db)
        conn = MongoClient(mongo_uri, datetime_conversion=DatetimeConversion.DATETIME_CLAMP)
    else:
        conn = MongoClient(host, port, datetime_conversion=DatetimeConversion.DATETIME_CLAMP)
    return conn[db]


def connect_mongo_local():
    return _connect_mongo(
        host=mongo_host_local,
        port=mongo_port_local,
        username=mongo_user_local,
        password=mongo_pass_local,
        auth_db=mongo_auth_db_local,
        db=mongo_db_local
    )


def replace_entity_to_local_db(prev_id, entity, db):
    collection = db[mongo_collection_local]
    entity['_id'] = entity[UUID]
    collection.replace_one({'_id': prev_id}, entity)


def get_by_id_from_local_db(entity_id, db):
    return db.find_one({'_id', entity_id})


def read_mongo(auth_db, db, collection, query, host, port, username, password, projection={}, skip=0, limit=100):
    db = _connect_mongo(host=host, port=port, username=username, password=password, auth_db=auth_db, db=db)
    cursor = db[collection].find(query, projection=projection, no_cursor_timeout=True, batch_size=10000).skip(skip)\
        .limit(limit)

    return cursor


def flatten_dict(entity, json_data, total_hash, hash_to_key, keys):
    """
    This method just iterates through a JSON entity and recursively traverses all the nodes in the structure doing
    concatenation of keys (actually for the sake of performance reasons and compatibility with the main algorithm it
    uses cryptographically reliable hash functions such as SHA-256 in order to get rid of string concatenation and use
    numbers instead).

    :param entity: an entity to iterate through.
    :param json_data: a container for data
    :param total_hash: a running total hash consisting of all the hashes of the keys participating in forming a
    concatenated path. E.g. path 'key1.key2.key3' having hash(key1) = 1, hash(key2) = 2, hash(key3) = 3 has its total
    hash 6 = 1 + 2 + 3.
    :param hash_to_key: a dictionary for mapping from the hash which is used for the sake of compatibility with the core
    algorithm to the key, the actual path hashed with a cryptographically reliable hash function.
    :param keys:
    :return: a side effect of populating json_data.
    """
    prime_number = 31
    for key, value in entity.items():
        k_hash = int.from_bytes(hashlib.sha256(key.encode('utf-8')).digest(), 'big')
        if isinstance(value, dict):
            total_hash += prime_number * k_hash
            flatten_dict(value, json_data, total_hash, hash_to_key, keys)
        elif isinstance(value, list):
            for index, index_value in enumerate(value):
                total_hash += prime_number * int.from_bytes(hashlib.sha256(str(index).encode('utf-8')).digest(), 'big')
                flatten_dict(index_value, json_data, total_hash, hash_to_key, keys)
        elif isinstance(value, ObjectId):
            json_data[UUID] = str(value)
        elif isinstance(value, datetime.datetime):
            continue
        else:
            if key == '_id' and total_hash == TOP_LEVEL_HASH:
                json_data[UUID] = value
            else:
                total_hash += prime_number * k_hash

                if keys.get(key, None) is None:
                    keys[key] = total_hash

                    if hash_to_key.get(total_hash, None) is None:
                        hash_to_key[total_hash] = {key}
                    else:
                        hash_to_key[total_hash].add(key)
                else:
                    total_hash = keys[key]

                json_data[str(total_hash)] = [str(value)]


def flatten_dict_simple(entity, json_data, total_key):
    """
    This method just iterates through a JSON entity and recursively traverses all the nodes in the structure doing
    concatenation of keys. For the sake of performance the concatenated key consists only of two last parts of its
    JSON-path as this shortened combination uniquely (in the current setup) defines a key. E.g. `primaryPhone.number00`.
    Also, it adds extra two zeroes at the end of the key for the sake of compliance with the main algorithm.

    :param entity: an entity to iterate through.
    :param json_data: a container for data
    :param total_key: a concatenated key which is used on the bottom of recursion as a json_data key.
    :return: a side effect of populating json_data.
    """
    for key, value in entity.items():
        if isinstance(value, dict):
            flatten_dict_simple(value, json_data, key)
        elif isinstance(value, list):
            for index, index_value in enumerate(value):
                new_total_key = key + '.' + str(index)
                flatten_dict_simple(index_value, json_data, new_total_key)
        elif isinstance(value, ObjectId):
            json_data[UUID] = str(value)
        else:
            if (key == '_id' or (key == 'eventId' and not json_data.get(UUID, None))) and total_key == TOP_LEVEL_KEY:
                json_data[UUID] = value
                continue
            if key in {'createdByUserId', 'createdByCompanyId', '_class', 'employeeOfCompanyId', 'citizenship'}:
                continue
            else:
                if 'Phone' in total_key and key != 'type':
                    total_key = 'primaryPhone'
                    for match in phonenumbers.PhoneNumberMatcher(value, 'RU'):
                        value = phonenumbers.format_number(match.number, phonenumbers.PhoneNumberFormat.E164)
                if 'messengers' in total_key:
                    total_key = 'messengers'
                new_total_key = total_key + '.' + key + KEY_SUFFIX
                if new_total_key not in {'top.personId00', 'person.userId00', 'fullName.name00', 'fullName.surname00',
                                         'russianPassport.series00', 'russianPassport.number00', 'inn.number00',
                                         'snils.number00', 'fullName.patronymic00', 'primaryPhone.number00',
                                         'registrationAddress.value00', 'primaryPhone.number00',
                                         'primaryEmail.email00', 'personalData.birthDate00'}:
                    continue
                if new_total_key in {'fullName.name00', 'fullName.surname00', 'fullName.patronymic00'}:
                    new_total_key = 'fullName.fio00'
                val_set = json_data.get(new_total_key, set())
                if new_total_key == 'fullName.fio00' and val_set:
                    value = val_set.pop() + ' ' + value
                # add non-empty values only
                if value:
                    val_set.add(value)
                if val_set:
                    json_data[new_total_key] = val_set


class MongoConnector(object):
    db_local = connect_mongo_local()

    def get_by_id_from_local_db(self, entity_id):
        return get_by_id_from_local_db(entity_id, self.db_local)

    def replace_entity_to_local_db(self, prev_id, entity):
        return replace_entity_to_local_db(prev_id, entity, self.db_local)


if __name__ == '__main__':
    read_data(limit=3000000, skip=4800000, json_data={}, file_name='prod200000last.json')
