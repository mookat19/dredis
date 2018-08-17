import hashlib
import json
import os.path
import shutil
import tempfile

from lupa import LuaRuntime


class RedisScriptError(Exception):
    """Indicate error from calls to redis.call()"""


class DiskKeyspace(object):

    def __init__(self):
        self.keys = {}
        self.directory = tempfile.mkdtemp(prefix="redis-test-")
        print("Directory = {}".format(self.directory))

    def _key_path(self, key):
        return os.path.join(self.directory, key)

    def flushall(self):
        try:
            shutil.rmtree(self.directory)
        except:
            pass
        try:
            os.makedirs(self.directory)
        except:
            pass

    def incrby(self, key, increment=1):
        key_path = self._key_path(key)
        number = 0
        if os.path.exists(key_path):
            with open(key_path, 'r') as f:
                content = f.read()
            number = int(content)
        result = str(number + increment)
        with open(key_path, 'w') as f:
            f.write(result)
        return result

    def exists(self, key):
        return os.path.exists(self._key_path(key))

    def get(self, key):
        with open(self._key_path(key), 'r') as f:
            return f.read()

    def set(self, key, value):
        with open(self._key_path(key), 'w') as f:
            f.write(value)

    def sadd(self, key, value):
        key_path = self._key_path(key)
        if not self.exists(key):
            os.makedirs(key_path)
        fname = hashlib.md5(value).hexdigest()
        value_path = os.path.join(key_path, fname)
        if os.path.exists(value_path):
            return 0
        else:
            with open(value_path, 'w') as f:
                f.write(value)
            return 1

    def smembers(self, key):
        result = set()
        key_path = self._key_path(key)
        if self.exists(key):
            for fname in os.listdir(key_path):
                with open(os.path.join(key_path, fname)) as f:
                    result.add(f.read())
        return result

    def sismember(self, key, value):
        key_path = self._key_path(key)
        fname = hashlib.md5(value).hexdigest()
        value_path = os.path.join(key_path, fname)
        return os.path.exists(value_path)

    def scard(self, key):
        return len(self.smembers(key))

    def delete(self, key):
        if self.exists(key):
            key_path = self._key_path(key)
            if os.path.isfile(key_path):
                os.remove(self._key_path(key))
            else:
                shutil.rmtree(key_path)
            return 1
        else:
            return 0

    def zadd(self, key, score, value):
        """
        # alternative
        /path/x/10
        /path/y/20 -> 10
        /path/z/30
        --------
        # alternative
        /path/10/x
        /path/20/y ->
        /path/30/z
        /path/1/y <-
        ------
        # alternative
        /path/10.txt -> x
        /path/20.txt -> y
        /path/30.txt -> z
        ------
        # current
        /path/scores/10 -> "x1\nx2"
        /path/scores/20 -> "y"
        /path/scores/30 -> "z"
        /path/values/hash(x) -> 1
        """
        key_path = self._key_path(key)
        scores_path = os.path.join(key_path, 'scores')
        values_path = os.path.join(key_path, 'values')
        if not os.path.exists(key_path):
            os.makedirs(scores_path)
            os.makedirs(values_path)

        score_path = os.path.join(scores_path, score)
        value_path = os.path.join(values_path, hashlib.md5(value).hexdigest())
        if os.path.exists(value_path):
            with open(value_path, 'r') as fvalue:
                previous_score = fvalue.read()
            if previous_score == score:
                return 0
            else:
                self._remove_line_from_file(score_path, skip_line=value)
                return 1
        else:
            with open(value_path, 'w') as f:
                f.write(score)
            with open(score_path, 'a') as f:
                f.write(value + '\n')
            return 1

    def _remove_line_from_file(self, score_path, skip_line):
        tempfd, tempfname = tempfile.mkstemp()
        with open(tempfname, 'w') as tfile:
            with open(score_path) as f:
                for line in f.readlines():
                    if line.strip() != skip_line:
                        tfile.write(line)
        os.close(tempfd)
        os.rename(tempfname, score_path)

    def zrange(self, key, start, stop, with_scores):
        key_path = self._key_path(key)
        lines = []
        scores_path = os.path.join(key_path, 'scores')
        if os.path.exists(scores_path):
            scores = sorted(os.listdir(scores_path), key=int)
            for score in scores:
                with open(os.path.join(scores_path, score)) as f:
                    sublist = sorted(line.strip() for line in f.readlines())
                    lines.extend(sublist)
        if stop < 0:
            stop = -stop
        end = len(lines) - stop + 1
        return lines[start:end]

    def zcard(self, key):
        key_path = self._key_path(key)
        values_path = os.path.join(key_path, 'values')
        if os.path.exists(values_path):
            return len(os.listdir(values_path))
        else:
            return 0

    def zscore(self, key, member):
        key_path = self._key_path(key)
        value_path = os.path.join(key_path, 'values', hashlib.md5(member).hexdigest())
        if os.path.exists(value_path):
            with open(value_path, 'r') as f:
                return f.read().strip()
        else:
            return None

    def eval(self, script, numkeys, keys):
        lua = LuaRuntime(unpack_returned_tuples=True)
        lua.execute('KEYS = {%s}' % ', '.join(map(json.dumps, keys)))
        redis_obj = RedisLua(self)
        redis_lua = lua.eval('function(redis) {} end'.format(script))
        return redis_lua(redis_obj)

    def zrem(self, key, *members):
        """
        /path/scores/10 -> "x1\nx2"
        /path/scores/20 -> "y"
        /path/scores/30 -> "z"
        /path/values/hash(x) -> 1
        """
        key_path = self._key_path(key)
        scores_path = os.path.join(key_path, 'scores')
        values_path = os.path.join(key_path, 'values')
        result = 0
        for member in members:
            value_path = os.path.join(values_path, hashlib.md5(member).hexdigest())
            if not os.path.exists(value_path):
                continue
            result += 1
            with open(value_path, 'r') as f:
                score = f.read().strip()
            score_path = os.path.join(scores_path, score)
            os.remove(value_path)
            self._remove_line_from_file(score_path, member)
        return result




class RedisLua(object):

    def __init__(self, keyspace):
        self._keyspace = keyspace

    def call(self, cmd, *args):
        try:
            method = getattr(self._keyspace, cmd)
            return method(*args)
        except AttributeError:
            raise RedisScriptError('@user_script: Unknown Redis command called from Lua script')
        except Exception as exc:
            raise RedisScriptError(str(exc))

    def pcall(self, cmd, *args):
        try:
            return self.call(cmd, *args)
        except Exception as exc:
            return {'err': 'ERR Error running script: {}'.format(str(exc))}