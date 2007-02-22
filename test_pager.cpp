#include "pager.h"
#include <stdio.h>
#include <string>

using namespace ST;
using namespace std;

int main(int argc, char **argv)
{
    uint32_t ptr;

    if (argc < 2)
    {
        puts("pager db");
        return 0;
    }

    Pager pgr;
    PageBuf pb(pgr);

    pgr.open(argv[1], Pager::OPEN_WRITE);
    printf("page count %d, page size %d\n", pgr.length(), pgr.pagesize());
    
    pgr.new_page(pb);
    sprintf((char*)pb.get_payload(), "i love you babey");
    ptr = pb.get_page();
    pgr.return_page(pb);
    pgr.close();

    pgr.open(argv[1], Pager::OPEN_WRITE);
    printf("page count %d, page size %d\n", pgr.length(), pgr.pagesize());
    pgr.read_page(pb, ptr);
    puts((char*)pb.get_payload());
    pgr.return_page(pb);

#define PS 100000
    uint32_t pages[PS];
    for (int i = 0; i < PS; i++)
    {
        pgr.new_page(pb);
        pages[i] = pb.get_page();
        sprintf((char*)pb.get_payload(), "hi, i'm page %u", pb.get_page());
    }
  
    for (int i = 0; i < PS; i++)
    {
        pgr.read_page(pb, pages[i]);
        //printf("%u == %s\n", pages[i], pb.get_payload());
        if (! (i % 2))
        {
            pgr.free_page(pb);
        }
    }

    printf("page count %d, page size %d\n", pgr.length(), pgr.pagesize());
    
    return 0;
}

