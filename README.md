leo_backend_db
==============

[![Build Status](https://secure.travis-ci.org/leo-project/leo_backend_db.png?branch=v2)](http://travis-ci.org/leo-project/leo_backend_db)

* **leo_backend_db** is wrapper of local KVS and uses Basho bitcask, Basho eleveldb and Erlang ETS.
  * [bitcask](https://github.com/basho/bitcask)
  * [eleveldb](https://github.com/basho/eleveldb)
  * [Erlang ETS](http://www.erlang.org/doc/man/ets.html)

## Build Information

* **leo_backend_db** uses [rebar](https://github.com/basho/rebar) build system. Makefile so that simply running "make" at the top level should work.
* **leo_backend_db** requires Erlang/OTP 19.3 or later.

## Usage

```
$ git clone git@github.com:leo-project/leo_backend_db.git
$ cd leo_backend_db
$ make
$ mkdir test_db
$ erl -pa ebin deps/*/ebin
Erlang/OTP 21 [erts-10.3] [source] [64-bit] [smp:12:12] [ds:12:12:10] [async-threads:1] [hipe]

Eshell V10.3  (abort with ^G)
1> leo_backend_db_api:new(test_leveldb, 2, leveldb, "test_db").
ok
2> leo_backend_db_api:put(test_leveldb, <<"key1">>, <<"val1">>).
ok
3> leo_backend_db_api:get(test_leveldb, <<"key1">>).
{ok,<<"val1">>}
4> leo_backend_db_api:stop(test_leveldb).
...
```

## Usage in LeoFS

**leo_backend_db** is used in [leo_object_storage](https://github.com/leo-project/leo_object_storage) and [leo_mq](https://github.com/leo-project/leo_mq).

### [leo_object_storage](https://github.com/leo-project/leo_object_storage)

**leo_backend_db** works to handle the backend database of LeoFS. It manages the stored objects in LeoFS.

### [leo_mq](https://github.com/leo-project/leo_mq)

**backend_db** is also used to manage message queue among the LeoFS storage nodes.

## Lincense

leo_backend_db's license is "Apache License Version 2.0".

## Sponsors

* LeoProject/LeoFS was sponsored by [Rakuten, Inc.](http://global.rakuten.com/corp/) from 2012 to Dec of 2018.
* LeoProject/LeoFS is sponsored by [Lions Data, Ltd](https://lions-data.com/) from Jan of 2019.
