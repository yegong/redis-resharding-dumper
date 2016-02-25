Redis Resharding Dumper
=======================

Inspired by `redis-resharding-proxy <https://github.com/smira/redis-resharding-proxy>`_.

Redis Resharding Dumper could be used to split (re-shard) instance of Redis into several smaller instances without interrupting
normal operations.

Introduction
------------

.. image:: https://raw.github.com/yegong/redis-resharding-dumper/master/redis-resharding.png
    :width: 500px

Redis resharding dumper is written in Go and requires no dependencies.

Compare to redis-resharding-proxy 
---------------------------------

+----------------------------+--------------------------+-------------------------+
|                            | redis-resharding-proxy   | redis-resharding-dumper |
+============================+==========================+=========================+
| Connection to old redis    | slave of old redis       | slave of old redis      |
+----------------------------+--------------------------+-------------------------+
| Connection to new redis    | master of new redis      | client of new redis     |
+----------------------------+--------------------------+-------------------------+
| How to resharding          | regex                    | not implemented. leave  |
|                            |                          | it to twemproxy         |
+----------------------------+--------------------------+-------------------------+
| RDB in SYNC                | parse and filter rdb     | parse rdb to set/hset.. |
|                            |                          | command(slower)         |
+----------------------------+--------------------------+-------------------------+
| Resharding multi server    | no                       | yes                     |
+----------------------------+--------------------------+-------------------------+



Installing/building
-------------------

If you have Go environment ready::

    go get github.com/yegong/redis-resharding-dumper

Otherwise install Go and set up environment::

    $ mkdir $HOME/go
    $ export GOPATH=$HOME/go
    $ export PATH=$PATH:$GOPATH/bin

After that you can run ``redis-resharding-dumper``.

Using
-----

``redis-resharding-dumper`` accepts several options::

  -master-host="localhost": Master Redis host
  -master-port=6379: Master Redis port
  -slave-host="localhost": Proxy listening interface, default is all interfaces
  -slave-port=6380: Proxy port for listening

They are used to configure proxy's listening address (which is used in Redis slave to connect to) and master Redis address.

Compatibility
-------------

Resharding proxy should be compatible with any Redis version.


Thanks
------

I would like to say thanks for ideas and inspiration to `Andrey Smirnov <https://github.com/smira>`_.

Copyright and Licensing
-----------------------

Copyright 2016 Cooper Du. Unless otherwise noted, the source files are distributed under the MIT License found in the LICENSE file.
