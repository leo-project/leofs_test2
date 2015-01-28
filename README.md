LeoFS Integration Test Tool
===========================

![LeoFS Logo](http://leo-project.net/leofs/docs/_static/leofs-logo-small.png)

Overview
--------

* ``leofs_test2`` is a integration test tool for LeoFS clusters. Tests/Scenarios depend on Erlang and can communicate with the cluster using the distributed Erlang.


Bootstrapping Your Test Environment
-----------------------------------

### Build ``leofs_test2``

* "leofs_test2" requires Erlang R16B03-1 or later.
* "leofs_test2" uses the rebar build system. Makefile so that simply running "make" at the top level should work.

```bash
$ git clone https://github.com/leo-project/leofs_test2.git
$ cd leofs_test2
$ make
```

### Modify ``/etc/hosts`` in order to create bucket(s) with ``erlcloud``

* Add a domain for the LeoFS bucket in ``/etc/hosts``

```bash
$ sudo vi /etc/hosts

## Replace {BUCKET_NAME} with the name of the bucket ##
127.0.0.1 localhost <BUCKET_NAME>.localhost
```

Starting leofs_test2
--------------------

* leofs_test2 has options as follows. You can set suitable option(s) with tests.

```bash
$ ./leofs_test --help
Usage: leofs_test [-b <bucket>] [-c <cookie>] [-d <leofs_dir>] [-k <keys>] [-m <manager>] [-t <test>] [-h] [-v]

  -b, --bucket   Target a bucket
  -c, --cookie   Distributed-cookie for communication with LeoFS
  -d, --dir      LeoFS directory
  -k, --keys     Total number of keys
  -m, --manager  LeoFS Manager
  -t, --test     Execute a test
  -h, --help     Show the program options
  -v, --version  Show version information
```

* Default value of options
    * bucket: "backup"
    * cookie: "401321b4"
    * keys: 10000
    * manager: 'manager_0@127.0.0.1'

* Example - Execute the scenarios

```bash
$ ./leofs_test -d <leofs-package-dir> -b <bucket> -k 10000 -c leofs_test

...

::: START :::

 [SCENARIO-1]
   * create_bucket (create a bucket)
   * put_objects (put objects)-------------------------------------------|
   * check_redundancies (check redundancies of replicas)-----------------|
   * del_objects (del objects)-------------------------------------------|
   * check_redundancies (check redundancies of replicas)-----------------|
 ...
 [SCENARIO-5]
   * remove_avs (remove avs of a node)
   * recover_node (recover data of a node)
   * watch_mq (watch state of mq)-----------------------------|
   * check_redundancies (check redundancies of replicas)-----------------|
::: Finished (1617sec) :::
```

Sponsors
--------

LeoProject/LeoFS is sponsored by [Rakuten, Inc.](http://global.rakuten.com/corp/) and [Rakuten Institute of Technology](http://rit.rakuten.co.jp/).