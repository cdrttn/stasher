#include "pager.h"
#include "bucket.h"
#include "bucketbuf.h"
#include "bucketarray.h"
#include "hashheaderbuf.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string>
#include <iostream>

using namespace ST;
using namespace std;

//16 MB
#define MAX_DATA 0xffffff
static const char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
#define CHARSET_LEN (sizeof(charset) - 1)

int array_size = 0;
char *filename = NULL;

void pgr_open(Pager &pgr, HashHeaderBuf &hhb, const char *what = "", int flags = Pager::OPEN_WRITE)
{
    pgr.open(filename, flags);
    printf("opened (%s) -> %s, pages -> %d\n", what, pgr.filename().c_str(), pgr.length());

    if (pgr.length() > 1)
    {
        //read in hhb
        pgr.read_page(hhb, 1);
        assert(hhb.validate());
    }
    else
    {
        //write out hhb
        hhb.allocate();
        hhb.clear();
        hhb.create();
        hhb.set_maxrec(16);
        hhb.set_capacity(16);
        hhb.set_maxexp(12);
        pgr.write_page(hhb, 1);
    }
}

void test_header()
{
    Pager pgr;
    HashHeaderBuf hhb(pgr);
    pgr_open(pgr, hhb, "test_header", Pager::OPEN_READ);

    printf("size -> %d\n", hhb.get_size());
    printf("maxrec -> %d\n", hhb.get_maxrec());
    printf("maxexp -> %d\n", hhb.get_maxexp()); 
    printf("capacity -> %d\n", hhb.get_capacity());
    BucketArray barr(hhb);
}

void add_array()
{
    Pager pgr;
    HashHeaderBuf hhb(pgr);
    pgr_open(pgr, hhb, "add_array");
    BucketArray barr(hhb);

    uint32_t key, value;
    key = value = 444;

    Bucket bucket;
    int i;
    for (i = 1; i <= array_size; i++)
    {
        barr.append(bucket); 
        bucket.append(i, &key, sizeof key, &value, sizeof value);
    }

    pgr.write_page(hhb);
}

void read_array()
{
    Pager pgr;
    HashHeaderBuf hhb(pgr);
    pgr_open(pgr, hhb, "read_array", Pager::OPEN_READ);
    BucketArray barr(hhb);

    printf("bucket array len -> %d\n", barr.length());
    assert(barr.length() == array_size);

    uint32_t key, value;
    Bucket bucket;
    int i;

    for (i = 0; i < array_size; i++)
    {
        barr.get(bucket, i);
        buffer k, v;
        BucketIter iter = bucket.iter();
        while (iter.next())
        {
            iter.get_key(k);
            iter.get_value(v);
            memcpy(&key, &k[0], sizeof key);
            memcpy(&value, &v[0], sizeof value);

            printf("hash32(%d), key(%d), value(%d)\n", iter.get_hash32(), key, value);
        }
    }
}

void resize_array(uint32_t bcount = 0)
{
    Pager pgr;
    HashHeaderBuf hhb(pgr);
    char buf[256];
    sprintf(buf, "resize_array(%d)", bcount); 
    pgr_open(pgr, hhb, buf);
    BucketArray barr(hhb);

    barr.resize(bcount);    
    
    pgr.write_page(hhb);
}

void random_array()
{
    Pager pgr;
    HashHeaderBuf hhb(pgr);
    pgr_open(pgr, hhb, "random_array");


    BucketArray barr(hhb);

    Bucket bucket;
    int i;
    for (i = 1; i <= array_size; i++)
    {
        int datasize = random() % MAX_DATA;
        buffer data(datasize + sizeof datasize);
        memcpy(&data[0], &datasize, sizeof datasize);

        printf("datasize[%d] -> %d\n", i, datasize);
       
        while (--datasize >= 0)
        {
            uint8_t c = charset[random() % CHARSET_LEN];
            data[datasize + sizeof datasize] = c; 
        }

        barr.append(bucket); 
        bucket.append(i, &i, sizeof i, &data[0], data.size());
    }

    pgr.write_page(hhb);

}

int main(int argc, char **argv)
{
    if (argc < 3)
    {
        puts("bucketarray file arraysize");
        return 0;
    }
    filename = argv[1];
    array_size = atoi(argv[2]);
   
    if (array_size == 0)
    {
        test_header();
        return 0;
    }

    if (FileIO::exists(filename))
        FileIO::unlink(filename);

    srandom(time(NULL));

    try
    {
        add_array();
        read_array();
        test_header();
        resize_array(1000);
        test_header();
        resize_array(500);
        test_header();
        resize_array(0);
        test_header();
        resize_array(100);
        test_header();
        resize_array(0);
        test_header();
        random_array();
        read_array();
        test_header();
        resize_array(100);
        test_header();
        resize_array(0);
        test_header();
    }
    catch (CException e)
    {
        printf("%s (%s)\n", e.err_string().c_str(), e.cerr_string().c_str());
    }
    catch (Exception e)
    {
        printf("%s\n", e.err_string().c_str());
    }

    return 0;
}

