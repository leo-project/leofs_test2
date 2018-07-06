# LeoFS Integration Test Tool

![LeoFS Logo](https://leo-project.net/leofs/docs-old/_static/leofs-logo-small.png)

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
 [{bucket,"test"},
  {leofs_dir,"/home/leofs/dev/projects/leo-project/leofs/package/"}]

::: START :::

 [SCENARIO-1]
   * mnesia (test mnesia backup/restore)
   * user_crud (test user related CRUD)
   * endpoint_crud (test endpoint related CRUD)
   * bucket_crud (test bucket related CRUD)
   * watch_mq (watch state of mq)
        #1
            * 'storage_0@127.0.0.1':[N/A]
            * 'storage_1@127.0.0.1':[N/A]
            * 'storage_2@127.0.0.1':[N/A]
            * 'storage_3@127.0.0.1':[N/A]
   * update_log_level (update log level of a node)
   * dump_ring (dump ring data to the local disk)
   * create_bucket (create a bucket)
   * update_consistency_level (update consistency level of a node)
   * purge_cache (remove a cache from each gateway)
   * recover_file (recover data of a file)
        #1
            * 'storage_0@127.0.0.1':[N/A]
            * 'storage_1@127.0.0.1':[N/A]
            * 'storage_2@127.0.0.1':[N/A]
            * 'storage_3@127.0.0.1':[N/A]
        #1
            * 'storage_0@127.0.0.1':[N/A]
            * 'storage_1@127.0.0.1':[N/A]
            * 'storage_2@127.0.0.1':[N/A]
            * 'storage_3@127.0.0.1':[N/A]
   * get_objects_not_found (get objects_not_found)....................................................................................................|
   * put_zero_byte_objects (put zero byte objects)....................................................................................................|
   * get_objects (get objects)....................................................................................................|
   * put_objects (put objects)....................................................................................................|
   * get_objects (get objects)....................................................................................................|
   * check_redundancies (check redundancies of replicas)....................................................................................................|
   * del_objects (remove objects)....................................................................................................|
   * check_redundancies (check redundancies of replicas)....................................................................................................|
   * mp_upload_normal (multipart upload)
   * mp_upload_normal_in_parallel (multipart upload in parallel)
   * mp_upload_abort (abort multipart upload)
        #1
            * 'storage_0@127.0.0.1':[{leo_delete_dir_queue_1,4}]
            * 'storage_1@127.0.0.1':[{leo_delete_dir_queue_1,2}]
            * 'storage_2@127.0.0.1':[{leo_delete_dir_queue_1,4}]
            * 'storage_3@127.0.0.1':[{leo_delete_dir_queue_1,3}]
        #2
            * 'storage_0@127.0.0.1':[{leo_delete_dir_queue_1,2}]
            * 'storage_1@127.0.0.1':[{leo_delete_dir_queue_1,1}]
            * 'storage_2@127.0.0.1':[{leo_delete_dir_queue_1,1}]
            * 'storage_3@127.0.0.1':[{leo_delete_dir_queue_1,3}]
        #3
            * 'storage_0@127.0.0.1':[{leo_delete_dir_queue_1,2}]
            * 'storage_1@127.0.0.1':[N/A]
            * 'storage_2@127.0.0.1':[N/A]
            * 'storage_3@127.0.0.1':[N/A]
        #4
            * 'storage_0@127.0.0.1':[N/A]
            * 'storage_1@127.0.0.1':[N/A]
            * 'storage_2@127.0.0.1':[N/A]
            * 'storage_3@127.0.0.1':[N/A]
   * mp_upload_invalid_complete (invalid complete multipart upload)
.
.
.
 [SCENARIO-7]
   * delete_bucket (delete a bucket)
   * watch_mq (watch state of mq)
        #1
            * 'storage_1@127.0.0.1':[{leo_delete_dir_queue_1,3165}]
            * 'storage_2@127.0.0.1':[{leo_delete_dir_queue_1,4399}]
            * 'storage_3@127.0.0.1':[{leo_delete_dir_queue_1,2679}]
            * 'storage_4@127.0.0.1':[{leo_delete_dir_queue_1,1844}]
        #2
            * 'storage_1@127.0.0.1':[{leo_delete_dir_queue_1,442}]
            * 'storage_2@127.0.0.1':[{leo_delete_dir_queue_1,713}]
            * 'storage_3@127.0.0.1':[{leo_delete_dir_queue_1,208}]
            * 'storage_4@127.0.0.1':[{leo_delete_dir_queue_1,1012}]
        #3
            * 'storage_1@127.0.0.1':[N/A]
            * 'storage_2@127.0.0.1':[N/A]
            * 'storage_3@127.0.0.1':[N/A]
            * 'storage_4@127.0.0.1':[{leo_delete_dir_queue_1,609}]
        #4
            * 'storage_1@127.0.0.1':[N/A]
            * 'storage_2@127.0.0.1':[N/A]
            * 'storage_3@127.0.0.1':[N/A]
            * 'storage_4@127.0.0.1':[N/A]
   * create_bucket (create a bucket)
   * get_objects_not_found (get objects_not_found)....................................................................................................|
   * put_inconsistent_objects (put inconsistent objects)....................................................................................................|
   * scrub_cluster (scrub the whole cluster)
        #1
            * 'storage_1@127.0.0.1':[{leo_per_object_queue,4470}]
            * 'storage_2@127.0.0.1':[N/A]
            * 'storage_3@127.0.0.1':[N/A]
            * 'storage_4@127.0.0.1':[N/A]
        #2
            * 'storage_1@127.0.0.1':[{leo_per_object_queue,2552}]
            * 'storage_2@127.0.0.1':[N/A]
            * 'storage_3@127.0.0.1':[N/A]
            * 'storage_4@127.0.0.1':[N/A]
        #3
            * 'storage_1@127.0.0.1':[{leo_per_object_queue,13}]
            * 'storage_2@127.0.0.1':[N/A]
            * 'storage_3@127.0.0.1':[N/A]
            * 'storage_4@127.0.0.1':[N/A]
        #4
            * 'storage_1@127.0.0.1':[N/A]
            * 'storage_2@127.0.0.1':[N/A]
            * 'storage_3@127.0.0.1':[N/A]
            * 'storage_4@127.0.0.1':[N/A]
   * check_redundancies (check redundancies of replicas)....................................................................................................|
::: Finished (817sec) :::
```

* Example - Execute only a test

```bash
$ ./leofs_test -d leofs/package -t check_redundancies
...
:: TEST: check_redundancies (check redundancies of replicas)::::

 [TEST]
   * check_redundancies (check redundancies of replicas)....................................................................................................|
::: Finished (13sec) :::
```

## Milestone

### v0.4: Improve the basic scenarios retrieved from leofs-adm's commands Phase 1 (LeoFS 1.4.2 will come out with v0.4 tests passed)

1. check whether mq-(suspend|resume|stats) works
2. check whether recover-(file|node|consistency) works
3. check whether compaction-(suspend|resume) works
4. check whether du works
5. check whether purge-cache works
6. check whether (backup|restore)-mnesia works
7. check whether dump-ring works
8. check whether update-consistency-level works
9. check whether update-log-level works
10. check whether user related functions work
11. check whether endpoint related functions work
12. check whether bucket related functions work
13. check whether rollback works
14. check whether multipart uploads work
15. move onto rebar3
16. add the scenario-7 for the scrub cluster usecase

### v0.5: Improve the basic scenarios retrieved from leofs-adm's commands Phase 2

1. check whether recover-(ring|cluster|disk)
2. check whether du detail works
3. check whether remove-gateway works
4. check whether update-managers works
5. check whether multi-dc related functions work
6. check whether chown-bucket works
7. check whether update-acl works

### v0.6: Implement the advanced scenarios (mainly in case of failures)

1. check whether leo_storage cluster works with one storage being dead
2. check whether leo_storage cluster fail-back
3. check whether leo_storage cluster respond an expected error in case # of replicas < N
4. check whether leo_storage cluster works while compaction is ongoing
5. check whether leo_storage cluster works while rebalance is ongoing
6. check whether leo_storage cluster works while compaction AND rebalance are ongoing


## Sponsors

LeoProject/LeoFS is sponsored by [Rakuten, Inc.](http://global.rakuten.com/corp/) and [Rakuten Institute of Technology](http://rit.rakuten.co.jp/).
