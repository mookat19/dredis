import struct

import lmdb
import plyvel

from dredis.path import Path


class KeyCodec(object):

    STRING_TYPE = 1
    SET_TYPE = 2
    SET_MEMBER_TYPE = 3
    HASH_TYPE = 4
    HASH_FIELD_TYPE = 5
    ZSET_TYPE = 6
    ZSET_VALUE_TYPE = 7
    ZSET_SCORE_TYPE = 8
    KEY_TYPES = [STRING_TYPE, SET_TYPE, HASH_TYPE, ZSET_TYPE]

    # type_id | key_length
    KEY_PREFIX_FORMAT = '>BI'
    KEY_PREFIX_LENGTH = struct.calcsize(KEY_PREFIX_FORMAT)
    ZSET_SCORE_FORMAT = '>d'
    ZSET_SCORE_FORMAT_LENGTH = struct.calcsize(ZSET_SCORE_FORMAT)

    KEY_PREFIX_STRUCT = struct.Struct(KEY_PREFIX_FORMAT)
    ZSET_SCORE_STRUCT = struct.Struct(ZSET_SCORE_FORMAT)

    # the key format using <key length + key> was inspired by the `blackwidow` project:
    # https://github.com/KernelMaker/blackwidow/blob/5abe9a3e3f035dd0d81f514e598f29c1db679a28/src/zsets_data_key_format.h#L44-L53
    # https://github.com/KernelMaker/blackwidow/blob/5abe9a3e3f035dd0d81f514e598f29c1db679a28/src/base_data_key_format.h#L37-L43
    #
    # LevelDB doesn't have column families like RocksDB, so the binary prefixes were created to distinguish object types

    def get_key(self, key, type_id):
        prefix = self.KEY_PREFIX_STRUCT.pack(type_id, len(key))
        return prefix + bytes(key)

    def encode_string(self, key):
        return self.get_key(key, self.STRING_TYPE)

    def encode_set(self, key):
        return self.get_key(key, self.SET_TYPE)

    def encode_set_member(self, key, value):
        return self.get_key(key, self.SET_MEMBER_TYPE) + bytes(value)

    def get_min_set_member(self, key):
        return self.get_key(key, self.SET_MEMBER_TYPE)

    def encode_hash(self, key):
        return self.get_key(key, self.HASH_TYPE)

    def encode_hash_field(self, key, field):
        return self.get_key(key, self.HASH_FIELD_TYPE) + bytes(field)

    def get_min_hash_field(self, key):
        return self.get_key(key, self.HASH_FIELD_TYPE)

    def encode_zset(self, key):
        return self.get_key(key, self.ZSET_TYPE)

    def encode_zset_value(self, key, value):
        return self.get_key(key, self.ZSET_VALUE_TYPE) + bytes(value)

    def encode_zset_score(self, key, value, score):
        return self.get_key(key, self.ZSET_SCORE_TYPE) + self.ZSET_SCORE_STRUCT.pack(float(score)) + bytes(value)

    def decode_key(self, key):
        type_id, key_length = self.KEY_PREFIX_STRUCT.unpack(key[:self.KEY_PREFIX_LENGTH])
        key_value = key[self.KEY_PREFIX_LENGTH:]
        return type_id, key_length, key_value

    def decode_zset_score(self, ldb_key):
        _, length, key_name = self.decode_key(ldb_key)
        return self.ZSET_SCORE_STRUCT.unpack(key_name[length:length + self.ZSET_SCORE_FORMAT_LENGTH])[0]

    def decode_zset_value(self, ldb_key):
        _, length, key_name = self.decode_key(ldb_key)
        return key_name[length + self.ZSET_SCORE_FORMAT_LENGTH:]

    def get_min_zset_score(self, key):
        return self.get_key(key, self.ZSET_SCORE_TYPE)

    def get_min_zset_value(self, key):
        return self.get_key(key, self.ZSET_VALUE_TYPE)


class LMDBBatch(object):
    def __init__(self, env):
        self._env = env
        self._put = {}
        self._delete = set()

    def put(self, key, value):
        self._put[key] = value

    def delete(self, key):
        try:
            del self._put[key]
        except KeyError:
            pass
        self._delete.add(key)

    def write(self):
        with self._env.begin(write=True) as tnx:
            for k, v in self._put.items():
                tnx.put(k, v)
            for k in self._delete:
                tnx.delete(k)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.write()
            return True


class LMDBBackend(object):
    """
    Implement a subset of the interface of plyvel.DB
    """

    def __init__(self, path, **options):
        self._env = lmdb.open(path, **options)

    def get(self, key, default=None):
        with self._env.begin() as tnx:
            return tnx.get(key, default)

    def put(self, key, value):
        with self._env.begin(write=True) as tnx:
            tnx.put(key, value)

    def delete(self, key):
        with self._env.begin(write=True) as tnx:
            tnx.delete(key)

    def write_batch(self):
        return LMDBBatch(self._env)

    def close(self):
        self._env.close()

    def iterator(self, prefix='', include_value=True):
        with self._env.begin() as t:
            c = t.cursor()
            if prefix and not c.set_range(prefix):
                return
            for k, v in c:
                if not k.startswith(prefix):
                    return
                if include_value:
                    yield k, v
                else:
                    yield k

    def __iter__(self):
        with self._env.begin() as t:
            c = t.cursor()
            for k, v in c:
                yield k, v


def leveldb_factory(path):
    return plyvel.DB(path, create_if_missing=True)


def lmdb_factory(path):
    gb = 2 ** 30
    return LMDBBackend(path, map_size=100*gb, map_async=True, writemap=True, readahead=False)


class DBManager(object):

    _DBS = {}

    def setup_dbs(self, root_dir):
        for db_id_ in range(16):
            db_id = str(db_id_)
            directory = Path(root_dir).join(db_id)
            self._assign_db(db_id, directory)

    def open_db(self, path):
        return lmdb_factory(bytes(path))

    def get_db(self, db_id):
        return self._DBS[str(db_id)]['db']

    def delete_dbs(self):
        for db_id in self._DBS:
            self.delete_db(db_id)

    def delete_db(self, db_id):
        db_id = str(db_id)
        self._DBS[db_id]['db'].close()
        self._DBS[db_id]['directory'].reset()
        self._assign_db(db_id, self._DBS[db_id]['directory'])

    def _assign_db(self, db_id, directory):
        self._DBS[db_id] = {
            'db': self.open_db(directory),
            'directory': directory,
        }


KEY_CODEC = KeyCodec()
DB_MANAGER = DBManager()