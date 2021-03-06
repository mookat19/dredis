## 1.0.2

* Set TCP_NODELAY flag to client sockets (https://github.com/Yipit/dredis/pull/16)

## 1.0.1

* Improve TCP communication (https://github.com/Yipit/dredis/pull/15)

## 1.0.0

* Change storage to use LevelDB instead of our own directory structure and file formats (https://github.com/Yipit/dredis/pull/12)


## 0.1.1

* Minimize send() calls


## 0.1.0

* New binary file format (https://github.com/Yipit/dredis/pull/6)
* Faster directory checks (https://github.com/Yipit/dredis/pull/5)


## 0.0.7

* Add dredis-snapshot script

## 0.0.6

* Optimize Lua initialization


## 0.0.5

Change ZREM to check for scores_path emptiness


## 0.0.4

* Minimize `write()` calls
* Remove score file when there are no lines left


## 0.0.3

* Fix mismatching ZADD reply of existing elements
* Performance improvements by using select.poll
* Use generator when parsing instructions instead of list (should yield faster)
* Use asyncore.dispatcher without buffering output
* Rewrite file in place when removing lines


## 0.0.2

* Re-upload 0.0.1 with README as Markdown


## 0.0.1

* Implement many commands and Lua scripting
* Data is stored on the filesystem using different directory structures for each data structure (string, set, hash, sorted set)
