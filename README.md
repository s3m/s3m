# s3m

[![codecov](https://codecov.io/gh/s3m/s3m/graph/badge.svg?token=Y2BQJUGPJ5)](https://codecov.io/gh/s3m/s3m)
[![crates.io](https://img.shields.io/crates/v/s3m.svg)](https://crates.io/crates/s3m)

**s3m** a command-line tool for storing streams of data in s3 buckets.

## Problem trying to solve

There are streams of data that can not be lost besides that when created,
they degrade the performance of running systems, for example, if the stream
is a backup of a database, every time the stream is produced it may lock the
entire cluster (depends on the options and tools used mysqldump/xtrabackup for
example), however, the effort to create the stream is proportional to the size
of the database, in most cases the bigger the database is the more time
and CPU and Memory are required.

In the case of backups, normally the streams are piped to a compression tool
and later put in the disk, in some cases writing to the existing disk where
the database is or to a remote mount endpoint, is not possible due to size
constraints and the compressed backup should be streamed to am s3 bucket (X
provider), therefore if for some reason the connection gets lost while streaming
almost before finishing, the whole backup procedure could be corrupted and in
worst scenario everything should start all over again.

The aim of **s3m** apart from trying to consume fewer resources is to make as
much as possible "fault-tolerant" the storage procedure of the incoming stream
so that even if the server lost network connectivity, the stream can be resumed
and continue the upload process where it was left without the need to start all
over again (no when input is from STDIN/pipe).

https://s3m.stream
