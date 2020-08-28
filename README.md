# s3m

**s3m** a "fault-tolerant" command-line tool for storing streams of data in s3 buckets.

[![crates.io](https://img.shields.io/crates/v/s3m.svg)](https://crates.io/crates/s3m)
[![Build Status](https://travis-ci.org/s3m/s3m.svg?branch=master)](https://travis-ci.org/s3m/s3m)
[![Build Status - develop](https://github.com/s3m/s3m/workflows/build/badge.svg?branch=develop)](https://github.com/s3m/s3m/actions)


## Problem trying to solve

There are streams of data that can not be lost besides that when created,
they degrade the performance of running systems, for example, if the stream
is a backup of a database, every time the stream is produced it may lock the
entire cluster (depends on the options and tools used mysqldump/xtrabackup for
example), however, the effort to create the stream is proportional to the size
of the database, in most of the cases the bigger the database is the more time
and CPU and Memory is required.

In the case of backups, normally the streams are piped to a compression tool
and later put in the disk, in some cases writing to the existing disk where
the database is or to a remote mount endpoint, is not possible due to size
constraints and the compressed backup should be streamed to am s3 bucket (X
provider), therefore if for some reason the connection gets lost while streaming
almost before finishing, the whole backup procedure could be corrupted and in
worst scenario everything should start all over again.

The aim of **s3m** is to make as much as possible "fault-tolerant" the storage
the procedure of the data stream to so that even if the server lost network
connectivity, the stream could still be received and queued so that when the
network re-establishes it can continue where it was left and resume the
upload without the need to start all over again.

### AbortMultipartUpload

This operation aborts a multipart upload:

    s3m ls -m <s3>/<bucket> | awk '{system("s3m rm <s3>/<bucket>/"$5 "-a "$4);}'
