#include "pager.h"
#include "bucketbuf.h"
#include <stdio.h>
#include <string>

using namespace ST;
using namespace std;

void write_test(const char *file)
{
    Pager pgr;

    pgr.open(file, true);
    printf("opened(write) -> %s, pages -> %d\n", pgr.filename().c_str(), pgr.length());
    BucketBuf bb(pgr, 4);
    bb.allocate();
    printf("record size -> %d\n", bb.record_size());
    
    Record rec; 
    int count = 0;
    bb.iter_rewind();
    while (bb.free_next())
    {
        char key[256] = {0};   
        char value[256] = {0};   
        sprintf(key, "test_key %d", count);
        sprintf(value, "value i am for %d", count);
        bb.record().set_payload((uint8_t*)key, sizeof(key), (uint8_t*)value, sizeof(value));
        bb.record().set_hash32(count);
        bb.record().set_overflow_next(count*11);
        bb.set_size(bb.get_size()+1);
        count++;
    }

    //manual record-by-record move copy1 -> copy2
    BucketBuf copy1(bb);
    BucketBuf copy2(bb.pager(), bb.max_records());
    copy2.allocate();
    while (copy1.iter_next() && copy2.free_next())
    {
        copy2.record() = copy1.record();
        copy2.set_size(copy2.get_size() + 1);
        copy1.remove_record(copy1.record());
    }
    
    //operator=, copy and delete test
    BucketBuf save(bb.pager(), bb.max_records());
    Record &saverec = save.record();

    bb.iter_rewind();
    save = bb;
    save.clear_payload();
    save.set_size(0);
    assert(save.free_next());

    assert(bb.get_record(rec, 2));
    saverec = rec;

    printf("Record pending removal -> hash32(%d), key(%s), value(%s)\n",
            rec.get_hash32(), rec.get_key(), rec.get_value());

    bb.remove_record(rec);

    printf("Copied record -> hash32(%d), key(%s), value(%s)\n",
            saverec.get_hash32(), saverec.get_key(), saverec.get_value());
    
    assert(bb.insert_record(rec));
    char *key = "booh";
    char *value = "bengol";
    rec.set_payload((uint8_t*)key, strlen(key)+1, (uint8_t*)value, strlen(value)+1);
    rec.set_overflow_next(2342);    

    pgr.write_page(bb, 1);    
}

void read_test(const char *file)
{
    Pager pgr;

    pgr.open(file, false);
    printf("opened(read) -> %s, pages -> %d\n", pgr.filename().c_str(), pgr.length());
    BucketBuf bb(pgr, 4);
    bb.allocate();

    pgr.read_page(bb, 1);
    printf("record size -> %d, record count -> %d\n", bb.record_size(), bb.get_size());

    const char *codes[] = {
        "RECORD_EMPTY", "RECORD_SMALL", "RECORD_MEDIUM", "RECORD_LARGE"
    };

    Record rec; 
    for (int i = 0; i < bb.max_records(); i++)
    {
        if (bb.get_record(rec, i))
        {
            //printf("key_len -> %d, value_len -> %d\n", 
                //rec.get_keysize(), rec.get_valuesize());
            printf("hash -> %d, overflow -> %d, key -> %s, value -> %s\n", 
                rec.get_hash32(), rec.get_overflow_next(), 
                rec.get_key(), rec.get_value());
            printf("\ttype (%s)\n", codes[rec.get_type()]);
        }
        else
            printf("record gap @%d (%s)\n", i, codes[rec.get_type()]);
    } 

}

int main(int argc, char **argv)
{
    if (argc < 2)
    {
        puts("bbuf file");
        return 0;
    }

    write_test(argv[1]);
    read_test(argv[1]);

    return 0;
}

