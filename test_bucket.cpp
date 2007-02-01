#include "pager.h"
#include "bucket.h"
#include "bucketbuf.h"
#include "overflowbuf.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string>
#include <iostream>

using namespace ST;
using namespace std;

#define END 6 // ^F char

int delspan = 15;
char *filename = NULL;
char *keyval = NULL;

int read_kv(FILE *fp, buffer &key, buffer &value)
{
    int c;
    buffer *data = &key;

    while ((c = getc(fp)) != EOF && c != END)
        key.push_back(c);

    while ((c = getc(fp)) != EOF && c != END)
        value.push_back(c);

    return feof(fp) || ferror(fp);
}

void write_bucket()
{
    Pager pgr;
    pgr.open(filename, Pager::OPEN_WRITE);
    printf("opened (write) -> %s, pages -> %d\n", pgr.filename().c_str(), pgr.length());

    BucketBuf *bb = new BucketBuf(pgr);
    bb->allocate();
    bb->clear();
    bb->create();
    pgr.write_page(*bb, 1);
    Bucket bucket(bb); 

    FILE *fp = fopen(keyval, "r");
    assert(fp);

    buffer key, value;
    int index = 0;
    while (!read_kv(fp, key, value))
    {
        bucket.append(++index, key, value);
        
        key.clear();
        value.clear();
    }

    pgr.write_page(*bb, 1);
    fclose(fp);
}

void iter_bucket(uint32_t start = 1)
{
    Pager pgr;
    pgr.open(filename, true);
    printf("opened (iter) -> %s, pages -> %d\n", pgr.filename().c_str(), pgr.length());

    BucketBuf *bb = new BucketBuf(pgr);
    pgr.read_page(*bb, start);
    Bucket bucket(bb); 
    BucketIter iter = bucket.iter();

    int total = 0;
    buffer key, value;
    while (iter.next())
    {
        printf("hash -> %d\n", iter.record().get_hash32());
        iter.get_key(key);
        fwrite(&key[0], key.size(), 1, stderr);
        putc(END, stderr);

        iter.get_value(value);
        fwrite(&value[0], value.size(), 1, stderr);
        putc(END, stderr);

        total++;
    }
    putc('\n', stderr);
    printf("Total -> %d\n", total);
}

void del_bucket()
{
    Pager pgr;
    pgr.open(filename, Pager::OPEN_WRITE);
    printf("opened (del) -> %s, pages -> %d\n", pgr.filename().c_str(), pgr.length());

    BucketBuf *bb = new BucketBuf(pgr);
    pgr.read_page(*bb, 1);
    Bucket bucket(bb); 

    int count = 0;
    int maxdel = 315;
    int walk = delspan;
    int del = 0;

    BucketIter iter = bucket.iter();

    while (iter.next())
    {
        //bucket.remove(iter, Bucket::NOCLEAN);
        if (del > 0)
        {
            printf("removing hash -> %d\n", iter.record().get_hash32());
            //bucket.remove(iter);
            bucket.remove(iter, Bucket::NOCLEAN);
            count++;
            del--;
        }
        else if (walk == 0)
        {
            walk = delspan;
            del = maxdel - 5;
        }
        else
            walk--;
    }

    printf("Total removed -> %d\n", count);
    
    bucket.compact();
}

void clear_bucket()
{
    Pager pgr;
    pgr.open(filename, Pager::OPEN_WRITE);
    printf("opened (clear) -> %s, pages -> %d\n", pgr.filename().c_str(), pgr.length());

    BucketBuf *bb = new BucketBuf(pgr);
    pgr.read_page(*bb, 1);
    Bucket bucket(bb); 

    bucket.clear();
    pgr.free_pages(bb->get_page(), 1);
}

uint32_t copy_bucket()
{
    Pager pgr;
    pgr.open(filename, Pager::OPEN_WRITE);
    printf("opened (copy) -> %s, pages -> %d\n", pgr.filename().c_str(), pgr.length());

    BucketBuf *first = new BucketBuf(pgr);
    Bucket bucket1(first); 
    pgr.read_page(*first, 1);

    BucketBuf *second = new BucketBuf(pgr);
    Bucket bucket2(second); 
    second->allocate();
    second->clear();
    second->create();

    uint32_t newptr = pgr.alloc_pages(1);
    pgr.write_page(*second, newptr);

    BucketIter from = bucket1.iter();
    BucketIter to = bucket2.iter();

    bool doo = true;
    while (from.next())
    {
        if (doo)
        {
            bucket2.copy_quick(to, from);
            bucket1.remove(from, Bucket::NOCLEAN | Bucket::KEEPOVERFLOW);
        }
        doo = !doo;
    }
   
    from = bucket1.iter();
    to = bucket2.iter();
    while (from.next())
    {
        bucket2.copy_quick(to, from);
        bucket1.remove(from, Bucket::NOCLEAN | Bucket::KEEPOVERFLOW);
    }

    bucket1.compact();
    assert(bucket1.empty());
    
    pgr.free_pages(first->get_page(), 1);
    
    return newptr;
}

int main(int argc, char **argv)
{
    if (argc < 2)
    {
        puts("bucket file keyval [delspan]");
        return 0;
    }
    filename = argv[1];
    keyval = argv[2];

    //if (argc >= 4)
    //    delspan = atoi(argv[3]);
    //assert(delspan > 0);
    
    if (FileIO::exists(filename))
        FileIO::unlink(filename);
   
    //srandom(time(NULL));
    write_bucket();
    iter_bucket();
    del_bucket();
    fprintf(stderr, "\nAfter Del\n");
    iter_bucket();

    clear_bucket();
    write_bucket();
    uint32_t newptr = copy_bucket();
    fprintf(stderr, "\nAfter Copy\n");
    iter_bucket(newptr);

    /*
    write_bucket();
    iter_bucket();
    //del_bucket();
    //iter_bucket();
    clear_bucket();
    ////iter_bucket();
    */

    return 0;
}

