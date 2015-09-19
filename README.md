leo_backend_db
==============

[![Build Status](https://secure.travis-ci.org/leo-project/leo_backend_db.png?branch=develop)](http://travis-ci.org/leo-project/leo_backend_db)

* **leo_backend_db** is wrapper of local KVS and uses Basho bitcask, Basho eleveldb and Erlang ETS.
  * [bitcask](https://github.com/basho/bitcask)
  * [eleveldb](https://github.com/basho/eleveldb)
  * [Erlang ETS](http://www.erlang.org/doc/man/ets.html)

## Build Information

* **leo_backend_db** uses [rebar](https://github.com/basho/rebar) build system. Makefile so that simply running "make" at the top level should work.
* **leo_backend_db** requires Erlang R16B03-1 or later.

## Usage

```
$ git clone git@github.com:leo-project/leo_backend_db.git
$ cd leo_backend_db
$ make
$ mkdir test_db
$ erl -pa ebin deps/*/ebin
Erlang/OTP 17 [erts-6.4] [source] [64-bit] [smp:8:8] [async-threads:10] [kernel-poll:false] [dtrace]

Eshell V6.4  (abort with ^G)
1> leo_backend_db_api:new(test_leveldb, 2, leveldb, "test_db"). # third argument can be one of 'bitcask', 'leveldb', and 'ets'.
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

LeoProject/LeoFS is sponsored by [Rakuten, Inc.](http://global.rakuten.com/corp/) and supported by [Rakuten Institute of Technology](http://rit.rakuten.co.jp/).