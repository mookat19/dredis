# DRedis: Disk-based Redis implementation

Redis is a great key-value database and it's extremely fast because it's in-memory.
Some people want Redis's rich data types without having to worry about the memory limitations. For those
that can afford slower performance and want unlimited storage, DRedis may be an alternative.

**WARNING: This project is still experimental and it doesn't implement all Redis commands!**



## Installing

Make sure to install the [LevelDB](https://github.com/google/leveldb) C++ library (`apt-get install libleveldb-dev` or `brew install leveldb`) and then run:

```shell
$ pip install dredis
```

## Running


```shell
$ dredis --dir /tmp/dredis-data
```

To know about all of the options, use `--help`:

```shell
$ dredis --help
usage: dredis [-h] [-v] [--host HOST] [--port PORT] [--dir DIR] [--debug]
              [--flushall]

optional arguments:
  -h, --help     show this help message and exit
  -v, --version  show program's version number and exit
  --host HOST    server host (defaults to 127.0.0.1)
  --port PORT    server port (defaults to 6377)
  --dir DIR      directory to save data (defaults to a temporary directory)
  --debug        enable debug logs
  --flushall     run FLUSHALL on startup
```


If you want to try it with docker locally (port 6377 on the host):

```shell
$ docker-compose up
```


## Supported Commands

Command signature                            | Type
---------------------------------------------|-----
COMMAND\*                                    | Server
FLUSHALL                                     | Server
FLUSHDB                                      | Server
DBSIZE                                       | Server
DEL key [key ...]                            | Keys
TYPE key                                     | Keys
KEYS pattern                                 | Keys
EXISTS key [key ...]                         | Keys
PING [msg]                                   | Connection
SELECT db                                    | Connection
SET key value                                | Strings
GET key                                      | Strings
INCR key                                     | Strings
INCRBY key increment                         | Strings
GETRANGE key start end                       | Strings
SADD key value [value ..]                    | Sets
SMEMBERS key                                 | Sets
SCARD key                                    | Sets
SISMEMBER key value                          | Sets
EVAL script numkeys [key ...] [arg ...]      | Scripting
ZADD key score member [score member ...]     | Sorted Sets
ZRANGE key start top [WITHSCORES]            | Sorted Sets
ZCARD key                                    | Sorted Sets
ZREM key member [member ...]                 | Sorted Sets
ZSCORE key member                            | Sorted Sets
ZRANK key member                             | Sorted Sets
ZCOUNT key min_score max_score               | Sorted Sets
ZRANGEBYSCORE key min_score max_score [WITHSCORES] [LIMIT offset count] | Sorted Sets
ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] | Sorted Sets
HSET key field value [field value ...]       | Hashes
HDEL key field [field ...]                   | Hashes
HSETNX key field value                       | Hashes
HGET key value                               | Hashes
HKEYS key                                    | Hashes
HVALS value                                  | Hashes
HLEN key                                     | Hashes
HINCRBY key field increment                  | Hashes
HGETALL key                                  | Hashes

\* `COMMAND`'s reply is incompatible at the moment, it returns a flat array with command names (their arity, flags, positions, or step count are not returned). 


## How is DRedis implemented

Initially DRedis had its own filesystem structure, but then it was converted to use [LevelDB](https://github.com/google/leveldb), which is a lot more reliable and faster.
Other projects implement similar features to what's available on DRedis, but they aren't what Yipit needed when the project started.
Some similar projects follow:

* https://github.com/Qihoo360/pika
* https://github.com/KernelMaker/blackwidow
* https://github.com/siddontang/ledisdb
* https://github.com/reborndb/qdb
* https://github.com/alash3al/redix
* https://github.com/meitu/titan


## Lua support

Lua is supported through the [lupa](https://github.com/scoder/lupa) library.


## Challenges

### Data Consistency

We are relying on LevelDB's consistency, no stress tests were performed.

### Cluster mode & Replication

Replication, key distribution, and cluster mode isn't supported.
If you want higher availability you can create multiple servers that share or replicate a disk (consistency may suffer when replicating).
Use DNS routing or a network load balancer to route requests properly.

### Backups

There are many solutions to back up files. DRedis will have no impact when backups are performed because it's done from the outside (different from Redis, which uses `fork()` to snapshot the data).
A straightforward approach is to have period backups to an object storage such as Amazon S3.

The commands SAVE or BGSAVE will be supported in the future to guarantee consistency when generating backups.

This project includes a snapshot utility (`dredis-snapshot`) to make it easier to back up data locally or to AWS S3.
Be aware that there may be consistency issues during the snapshot (`dredis` won't pause during the temporary copy of the data directory).


## Why Python

Because it's a good language to get things off the ground quickly and @hltbra knows it very well.
If this becomes a valuable project, other languages will be evaluated — the language of choice won't affect much of the I/O bottleneck, though. 

Python 3 will be eventually supported, @hltbra didn't want to deal with bytes/str shenanigans when this project started as a proof-of-concept.


## Didn't you have better names?

[@andrewgross](https://github.com/andrewgross) suggested [REDISK](https://twitter.com/awgross/status/1031962830633934849). The name will only matter if this project survives, it's still an experiment.
Also, [other projects use the name redisk](https://github.com/search?q=redisk&type=Repositories).

[@nadlerjessie](https://github.com/nadlerjessie) is okay with the name dredis but wants it to be pronunced "Doctor Redis". The YipitData engineering team has fun with this pronunciation.
