#include "pager.h"
#include <stdio.h>
#include <string>

using namespace ST;
using namespace std;

#define BIGTEST 1000000

void write_pages(Pager &pgr, uint32_t offset, uint32_t span, char *what)
{
    BasicBuf pb(pgr);
    pb.allocate();

    //printf("%s: allocating %d pages starting @%d\n", what, span, offset);
    
    for (int i = 0; i < span; i++)
    {
        sprintf((char*)pb.get_buf(), "%s page(%d), start(%d), unit(%d)", what, offset+i, offset, i);
        pgr.write_page(pb, offset+i);
    }

}

int main(int argc, char **argv)
{
    if (argc < 2)
    {
        puts("pager file");
        return 0;
    }

    Pager pgr;
    pgr.open(argv[1], Pager::OPEN_WRITE);
    printf("opened -> %s, pages -> %d\n", pgr.filename().c_str(), pgr.length());
   
    uint32_t offset = pgr.alloc_pages(15);
    write_pages(pgr, offset, 15, "ALLOC_FIRST");
  
    puts("freeing 4, 5");
    pgr.free_pages(4, 5);
    offset = pgr.alloc_pages(3);
    write_pages(pgr, offset, 3, "ALLOC_SECOND");

    offset = pgr.alloc_pages(1);
    write_pages(pgr, offset, 1, "ALLOC_THIRD");
   
    offset = pgr.alloc_pages(3);
    write_pages(pgr, offset, 3, "ALLOC_FOURTH");

    offset = pgr.alloc_pages(1);
    write_pages(pgr, offset, 1, "ALLOC_FIFTH");

    uint32_t len = pgr.length();
    //offset = len - 5;
    puts("freeing some bottom pages (should truncate)");
    pgr.free_pages(len-5, 4);
    pgr.free_pages(len-1, 1);

    printf("starting len -> %d, ending len -> %d\n", len, pgr.length());

    printf("freelist load testing\n");
    offset = pgr.alloc_pages(BIGTEST);
    
    for (int i = 0; i < BIGTEST; i++)
    {
        char buf[128];
        sprintf(buf, "ALLOC_BIG %d", i);

        write_pages(pgr, offset + i, 1, buf);
    }
   
    for (int i = 0; i < BIGTEST; i += 2)
    {
        pgr.free_pages(offset + i, 1);
    }
   
    //reopen
    Pager pgr2;
    pgr2.open(argv[1], Pager::OPEN_WRITE);
    printf("opened -> %s, pages -> %d\n", pgr.filename().c_str(), pgr.length());

    
    return 0;
}

