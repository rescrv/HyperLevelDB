// Copyright (c) 2013, Cornell University
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//     * Neither the name of Replicant nor the names of its contributors may be
//       used to endorse or promote products derived from this software without
//       specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

// POSIX
#include <errno.h>

// LevelDB
#include <hyperleveldb/cache.h>
#include <hyperleveldb/db.h>
#include <hyperleveldb/filter_policy.h>

// po6
#include <po6/error.h>

// e
#include <e/guard.h>
#include <e/time.h>

static int
process_file(const char* filename, leveldb::DB* db)
{
    FILE* fin = fopen(filename, "r");

    if (!fin)
    {
        fprintf(stderr, "could not open %s: %s\n", filename, strerror(errno));
        return EXIT_FAILURE;
    }

    uint64_t t_start = e::time();
    uint64_t processed = 0;

    while (true)
    {
        char* line = NULL;
        size_t line_sz = 0;
        ssize_t amt = getline(&line, &line_sz, fin);

        if (amt < 0)
        {
            if (ferror(fin) != 0)
            {
                fprintf(stderr, "could not read from %s: %s\n", filename, strerror(ferror(fin)));
                fclose(fin);
                return EXIT_FAILURE;
            }

            if (feof(fin) != 0)
            {
                break;
            }

            fprintf(stderr, "unknown error when reading from %s\n", filename);
            fclose(fin);
            return EXIT_FAILURE;
        }

        if (amt < 1)
        {
            free(line);
            fprintf(stderr, "skipping blank line in %s\n", filename);
            continue;
        }

        /* wipe out the newline */
        line[amt - 1] = '\0';
        char* tmp = strchr(line, ' ');

        if (!tmp)
        {
            free(line);
            fprintf(stderr, "skipping invalid line in %s\n", filename);
            continue;
        }

        *tmp = '\0';

        /* issue a "get" operation */
        char* k = line;
        size_t k_sz = tmp - k;
        std::string val;
        leveldb::ReadOptions ropts;
        leveldb::Status rst = db->Get(ropts, leveldb::Slice(k, k_sz), &val);

        /* issue a "put" operation */
        char* v = tmp + 1;
        size_t v_sz = (line + amt - 1) - v;
        leveldb::WriteOptions wopts;
        wopts.sync = false;
        leveldb::Status wst = db->Put(wopts, leveldb::Slice(k, k_sz), leveldb::Slice(v, v_sz));
        free(line);

        if (!wst.ok())
        {
            return EXIT_FAILURE;
        }

        ++processed;
    }

    uint64_t t_end = e::time();
    fclose(fin);
    double tput = processed;
    tput = tput / ((t_end - t_start) / 1000000000.);
    fprintf(stdout, "processd %s: %ld ops in %f seconds = %f ops/second\n", filename, processed, (t_end - t_start) / 1000000000., tput);
    return EXIT_SUCCESS;
}

int
main(int argc, const char* argv[])
{
    leveldb::Options opts;
    opts.create_if_missing = true;
    opts.write_buffer_size = 64ULL * 1024ULL * 1024ULL;
    opts.filter_policy = leveldb::NewBloomFilterPolicy(10);
    //opts.block_cache = leveldb::NewLRUCache(64ULL * 1024ULL * 1024ULL);
    opts.block_size = 65536;
    leveldb::DB* db;
    leveldb::Status st = leveldb::DB::Open(opts, ".", &db);

    if (!st.ok())
    {
        std::cerr << "could not open LevelDB: " << st.ToString() << std::endl;
        return EXIT_FAILURE;
    }

    for (int i = 1; i < argc; ++i)
    {
        int rc = process_file(argv[i], db);

        if (rc != EXIT_SUCCESS)
        {
            delete db;
            return rc;
        }
    }

    std::string tmp;
    if (db->GetProperty("leveldb.stats", &tmp)) std::cout << tmp << std::endl;

    delete db;
    return EXIT_SUCCESS;
}
