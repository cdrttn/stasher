#include "pager.h"
#include "bucketbuf.h"
#include <stdio.h>
#include <string>

using namespace ST;
using namespace std;

void write_test(const char *file)
{
    char key[255], value[255];
    Pager pgr;

    pgr.open(file, Pager::OPEN_WRITE);
    printf("opened(write) -> %s, pages -> %d\n", pgr.filename().c_str(), pgr.length());
    BucketBuf bb(pgr);
    bb.allocate();
    bb.create();

    Record rec;
    int i;

    i = 0;
    do 
    {
        sprintf(key, "this is key %d", i);
        sprintf(value, "this is value %d", i);

        rec.set_keysize(strlen(key) + 1);
        rec.set_valuesize(strlen(value) + 1);
        rec.set_key((uint8_t *)key);
        rec.set_value((uint8_t *)value);
        rec.set_type(pgr.pagesize());

        i++;
    } while (bb.insert_record(rec));

    bb.first_record(rec);
    i = 0;
    while (bb.next_record(rec))
    {
        if (i++ > 89)
            bb.remove_record(rec);
    }

    bb.first_record(rec);
    bb.next_record(rec);
    char *d = "over value !!!";
    bb.insert_dup(rec, d, strlen(d)+1);
    bb.insert_dup(rec, d, strlen(d)+1);

    bb.next_record(rec);
    bb.next_record(rec);
    bb.next_record(rec);
    bb.insert_dup(rec, d, strlen(d)+1);
    bb.insert_dup(rec, d, strlen(d)+1);
    bb.insert_dup(rec, d, strlen(d)+1);
    bb.insert_dup(rec, d, strlen(d)+1);

    bb.first_record(rec);
    bb.next_record(rec);
    bb.next_record(rec);
    bb.next_record(rec);
    const uint8_t *val;
    uint16_t len;
    
    for (i=0,rec.first_value(val); i < 0 && rec.check_value(val, len); i++, bb.remove_dup(rec, val, len)) 
        printf("value(%s), len(%d)\n", val, len);
 
    char fugdup[256];
    for (i=0; i < 5; i++)
    {
        sprintf(fugdup, "dodo dup %d", i);
        bb.insert_dup(rec, fugdup, strlen(fugdup)+1);
    }

    for (i=0,rec.first_value(val); rec.check_value(val, len); i++)
        if (i%2) rec.next_value(val, len);
        else bb.remove_dup(rec, val, len);
    
    uint32_t pptr = pgr.alloc_pages(1);
    pgr.write_page(bb, pptr);
    pgr.close();
}

void read_test(const char *file)
{
    Pager pgr;

    pgr.open(file);
    printf("opened(read) -> %s, pages -> %d\n", pgr.filename().c_str(), pgr.length());
    BucketBuf bb(pgr);
    bb.allocate();
    pgr.read_page(bb, 1);

    Record rec;

    bb.first_record(rec);
    while (bb.next_record(rec))
    {
        const uint8_t *val;
        uint16_t len;
   
        printf("%s\n", rec.get_key());
        for (rec.first_value(val); rec.check_value(val, len); rec.next_value(val, len))
            printf("  -> %s len(%d)\n", val, len);
    }

    pgr.close();
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

