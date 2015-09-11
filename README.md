# LeoFS Integration Test Tool

![LeoFS Logo](http://leo-project.net/leofs/docs/_static/leofs-logo-small.png)

## Overview

* ``leofs_test2`` is a integration test tool for LeoFS clusters. Tests/Scenarios depend on Erlang and can communicate with the cluster using the distributed Erlang.


## Bootstrapping Your Test Environment

### Build "leofs_test2"

* "leofs_test2" requires Erlang R16B03-1 or later.
* "leofs_test2" uses the rebar build system. Makefile so that simply running "make" at the top level should work.

```bash
$ git clone https://github.com/leo-project/leofs_test2.git
$ cd leofs_test2
$ make
```

### Modify "/etc/hosts" in order to create bucket(s) with "erlcloud"

* Add a domain for the LeoFS bucket in ``/etc/hosts``

```bash
$ sudo vi /etc/hosts

## Replace {BUCKET_NAME} with the name of the bucket ##
127.0.0.1 localhost <BUCKET_NAME>.localhost
```

* Build a LeoFS of your test with "bootstrap.sh" which is included in [LeoFS source code](https://github.com/leo-project/leofs)

```bash
$ cd <leofs-root-dir>/
$ sh bootstrap.sh build integration-test
```


## Starting leofs_test2

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

* Example - Execute only a test

```bash
$ ./leofs_test -d /home/yosuke/dev/leo/test/leofs-1.2.5-dev/package -t check_redundancies
...
:: TEST: check_redundancies (check redundancies of replicas)::::

 [TEST]
   * check_redundancies (check redundancies of replicas)-----------------|
::: Finished (13sec) :::
```

## Milestone

### v0.4: Improve the basic scenarios retrieved from leofs-adm's commands

1. check whether mq-(suspend|resume) works
2. check whether recover-(file|ring|cluster) works
3. check whether compaction-(suspend|resume) works
4. check whether du (detail) works
5. check whether purge-cache works
6. check whether remove-gateway works
7. check whether (backup|restore)-mnesia works
8. check whether update-managers works
9. check whether dump-ring works
10. check whether user related functions work
11. check whether endpoint related functions work
12. check whether bucket related functions work
13. check whether multi-dc related functions work

### v0.6: Implement the advanced scenarios (mainly in case of failures)

1. check whether leo_storage cluster works with one storage being dead
2. check whether leo_storage cluster fail-back
3. check whether leo_storage cluster respond an expected error in case # of replicas < N
4. check whether leo_storage cluster works while compaction is ongoing
5. check whether leo_storage cluster works while rebalance is ongoing
6. check whether leo_storage cluster works while compaction AND rebalance are ongoing


## Sponsors

LeoProject/LeoFS is sponsored by [Rakuten, Inc.](http://global.rakuten.com/corp/) and [Rakuten Institute of Technology](http://rit.rakuten.co.jp/).