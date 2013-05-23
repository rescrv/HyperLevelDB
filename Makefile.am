## Copyright (c) 2013
## All rights reserved.
##
## Redistribution and use in source and binary forms, with or without
## modification, are permitted provided that the following conditions are met:
##
##     * Redistributions of source code must retain the above copyright notice,
##       this list of conditions and the following disclaimer.
##     * Redistributions in binary form must reproduce the above copyright
##       notice, this list of conditions and the following disclaimer in the
##       documentation and/or other materials provided with the distribution.
##     * Neither the name of nb nor the names of its contributors may be used to
##		 endorse or promote products derived from this software without specific
##		 prior written permission.
##
## THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
## AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
## IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
## ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
## LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
## CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
## SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
## INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
## CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
## ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
## POSSIBILITY OF SUCH DAMAGE.

ACLOCAL_AMFLAGS = -I m4 ${ACLOCAL_FLAGS}
AM_CFLAGS = -I$(abs_top_srcdir)/include -DLEVELDB_PLATFORM_POSIX -fno-builtin-memcmp -fno-builtin-memmove
AM_CXXFLAGS = -I$(abs_top_srcdir)/include -DLEVELDB_PLATFORM_POSIX -fno-builtin-memcmp -fno-builtin-memmove
AM_MAKEFLAGS = --no-print-directory

EXTRA_DIST =
EXTRA_DIST += AUTHORS
EXTRA_DIST += LICENSE
EXTRA_DIST += NEWS
EXTRA_DIST += README
EXTRA_DIST += TODO
EXTRA_DIST += port/README

leveldbdir = $(includedir)/leveldb
leveldb_HEADERS =
leveldb_HEADERS += include/leveldb/cache.h
leveldb_HEADERS += include/leveldb/c.h
leveldb_HEADERS += include/leveldb/comparator.h
leveldb_HEADERS += include/leveldb/db.h
leveldb_HEADERS += include/leveldb/env.h
leveldb_HEADERS += include/leveldb/filter_policy.h
leveldb_HEADERS += include/leveldb/iterator.h
leveldb_HEADERS += include/leveldb/options.h
leveldb_HEADERS += include/leveldb/slice.h
leveldb_HEADERS += include/leveldb/status.h
leveldb_HEADERS += include/leveldb/table_builder.h
leveldb_HEADERS += include/leveldb/table.h
leveldb_HEADERS += include/leveldb/write_batch.h
noinst_HEADERS =
noinst_HEADERS += db/builder.h
noinst_HEADERS += db/dbformat.h
noinst_HEADERS += db/db_impl.h
noinst_HEADERS += db/db_iter.h
noinst_HEADERS += db/filename.h
noinst_HEADERS += db/log_format.h
noinst_HEADERS += db/log_reader.h
noinst_HEADERS += db/log_writer.h
noinst_HEADERS += db/memtable.h
noinst_HEADERS += db/skiplist.h
noinst_HEADERS += db/snapshot.h
noinst_HEADERS += db/table_cache.h
noinst_HEADERS += db/version_edit.h
noinst_HEADERS += db/version_set.h
noinst_HEADERS += db/write_batch_internal.h
noinst_HEADERS += helpers/memenv/memenv.h
noinst_HEADERS += port/atomic_pointer.h
noinst_HEADERS += port/port_example.h
noinst_HEADERS += port/port.h
noinst_HEADERS += port/port_posix.h
noinst_HEADERS += port/thread_annotations.h
noinst_HEADERS += port/win/stdint.h
noinst_HEADERS += table/block_builder.h
noinst_HEADERS += table/block.h
noinst_HEADERS += table/filter_block.h
noinst_HEADERS += table/format.h
noinst_HEADERS += table/iterator_wrapper.h
noinst_HEADERS += table/merger.h
noinst_HEADERS += table/two_level_iterator.h
noinst_HEADERS += util/arena.h
noinst_HEADERS += util/coding.h
noinst_HEADERS += util/crc32c.h
noinst_HEADERS += util/hash.h
noinst_HEADERS += util/histogram.h
noinst_HEADERS += util/logging.h
noinst_HEADERS += util/mutexlock.h
noinst_HEADERS += util/posix_logger.h
noinst_HEADERS += util/random.h
noinst_HEADERS += util/testharness.h
noinst_HEADERS += util/testutil.h

lib_LTLIBRARIES = libleveldb.la

libleveldb_la_SOURCES = 
libleveldb_la_SOURCES += db/builder.cc
libleveldb_la_SOURCES += db/c.cc
libleveldb_la_SOURCES += db/dbformat.cc
libleveldb_la_SOURCES += db/db_impl.cc
libleveldb_la_SOURCES += db/db_iter.cc
libleveldb_la_SOURCES += db/filename.cc
libleveldb_la_SOURCES += db/log_reader.cc
libleveldb_la_SOURCES += db/log_writer.cc
libleveldb_la_SOURCES += db/memtable.cc
libleveldb_la_SOURCES += db/repair.cc
libleveldb_la_SOURCES += db/table_cache.cc
libleveldb_la_SOURCES += db/version_edit.cc
libleveldb_la_SOURCES += db/version_set.cc
libleveldb_la_SOURCES += db/write_batch.cc
libleveldb_la_SOURCES += table/block_builder.cc
libleveldb_la_SOURCES += table/block.cc
libleveldb_la_SOURCES += table/filter_block.cc
libleveldb_la_SOURCES += table/format.cc
libleveldb_la_SOURCES += table/iterator.cc
libleveldb_la_SOURCES += table/merger.cc
libleveldb_la_SOURCES += table/table_builder.cc
libleveldb_la_SOURCES += table/table.cc
libleveldb_la_SOURCES += table/two_level_iterator.cc
libleveldb_la_SOURCES += util/arena.cc
libleveldb_la_SOURCES += util/bloom.cc
libleveldb_la_SOURCES += util/cache.cc
libleveldb_la_SOURCES += util/coding.cc
libleveldb_la_SOURCES += util/comparator.cc
libleveldb_la_SOURCES += util/crc32c.cc
libleveldb_la_SOURCES += util/env.cc
libleveldb_la_SOURCES += util/env_posix.cc
libleveldb_la_SOURCES += util/filter_policy.cc
libleveldb_la_SOURCES += util/hash.cc
libleveldb_la_SOURCES += util/histogram.cc
libleveldb_la_SOURCES += util/logging.cc
libleveldb_la_SOURCES += util/options.cc
libleveldb_la_SOURCES += util/status.cc 
libleveldb_la_SOURCES += port/port_posix.cc
libleveldb_la_LDFLAGS = -pthread

TESTUTIL = util/testutil.cc
TESTHARNESS = util/testharness.cc $(TESTUTIL)

noinst_PROGRAMS =
noinst_PROGRAMS += db_bench
noinst_PROGRAMS += leveldbutil

EXTRA_PROGRAMS =
EXTRA_PROGRAMS += db_bench_sqlite3
EXTRA_PROGRAMS += db_bench_tree_db

check_PROGRAMS =
check_PROGRAMS += arena_test
check_PROGRAMS += bloom_test
check_PROGRAMS += c_test
check_PROGRAMS += cache_test
check_PROGRAMS += coding_test
check_PROGRAMS += corruption_test
check_PROGRAMS += crc32c_test
check_PROGRAMS += db_test
check_PROGRAMS += dbformat_test
check_PROGRAMS += env_test
check_PROGRAMS += filename_test
check_PROGRAMS += filter_block_test
check_PROGRAMS += log_test
check_PROGRAMS += skiplist_test
check_PROGRAMS += table_test
check_PROGRAMS += version_edit_test
check_PROGRAMS += version_set_test
check_PROGRAMS += write_batch_test

TESTS = $(check_PROGRAMS)

db_bench_SOURCES = db/db_bench.cc $(TESTUTIL)
db_bench_LDADD = libleveldb.la -lpthread

db_bench_sqlite3_SOURCES = doc/bench/db_bench_sqlite3.cc $(TESTUTIL)
db_bench_sqlite3_LDADD = -lsqlite3

db_bench_tree_db_SOURCES = doc/bench/db_bench_tree_db.cc $(TESTUTIL)
db_bench_tree_db_LDADD = -lkyotocabinet

leveldbutil_SOURCES = db/leveldb_main.cc
leveldbutil_LDADD = libleveldb.la -lpthread

arena_test_SOURCES = util/arena_test.cc $(TESTHARNESS)
arena_test_LDADD = libleveldb.la -lpthread

bloom_test_SOURCES = util/bloom_test.cc $(TESTHARNESS)
bloom_test_LDADD = libleveldb.la -lpthread

c_test_SOURCES = db/c_test.c $(TESTHARNESS)
c_test_LDADD = libleveldb.la -lpthread

cache_test_SOURCES = util/cache_test.cc $(TESTHARNESS)
cache_test_LDADD = libleveldb.la -lpthread

coding_test_SOURCES = util/coding_test.cc $(TESTHARNESS)
coding_test_LDADD = libleveldb.la -lpthread

corruption_test_SOURCES = db/corruption_test.cc $(TESTHARNESS)
corruption_test_LDADD = libleveldb.la -lpthread

crc32c_test_SOURCES = util/crc32c_test.cc $(TESTHARNESS)
crc32c_test_LDADD = libleveldb.la -lpthread

db_test_SOURCES = db/db_test.cc $(TESTHARNESS)
db_test_LDADD = libleveldb.la -lpthread

dbformat_test_SOURCES = db/dbformat_test.cc $(TESTHARNESS)
dbformat_test_LDADD = libleveldb.la -lpthread

env_test_SOURCES = util/env_test.cc $(TESTHARNESS)
env_test_LDADD = libleveldb.la -lpthread

filename_test_SOURCES = db/filename_test.cc $(TESTHARNESS)
filename_test_LDADD = libleveldb.la -lpthread

filter_block_test_SOURCES = table/filter_block_test.cc $(TESTHARNESS)
filter_block_test_LDADD = libleveldb.la -lpthread

log_test_SOURCES = db/log_test.cc $(TESTHARNESS)
log_test_LDADD = libleveldb.la -lpthread

table_test_SOURCES = table/table_test.cc $(TESTHARNESS)
table_test_LDADD = libleveldb.la -lpthread

skiplist_test_SOURCES = db/skiplist_test.cc $(TESTHARNESS)
skiplist_test_LDADD = libleveldb.la -lpthread

version_edit_test_SOURCES = db/version_edit_test.cc $(TESTHARNESS)
version_edit_test_LDADD = libleveldb.la -lpthread

version_set_test_SOURCES = db/version_set_test.cc $(TESTHARNESS)
version_set_test_LDADD = libleveldb.la -lpthread

write_batch_test_SOURCES = db/write_batch_test.cc $(TESTHARNESS)
write_batch_test_LDADD = libleveldb.la -lpthread
