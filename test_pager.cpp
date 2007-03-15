#include "pager.h"
#include "pagelist.h"
#include <stdio.h>
#include <string>
#include <vector>
#include <list>

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
    PageBuf pb;

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

#define PS 100
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

    //copy constructor test
    pgr.read_page(pb, 5);
    assert(pb);
    printf("main buf = %s\n", pb.get_payload());
    vector<PageBuf> pbv;
    for (int i = 0; i < PS; i++)
        pbv.push_back(pb);       
    for (int i = 0; i < PS; i++)
    {
        assert(pbv[i].get_buf() == pb.get_buf());
        printf("buf = %s\n", pbv[i].get_payload());
    }

    PageBuf tmp;
    pgr.new_page_tmp(tmp);
    assert(tmp.get_page() == 0);
    assert(0 == tmp.get_page());
    assert(!tmp);
    memcpy(tmp.get_payload(), pb.get_payload(), pb.get_payloadsize());
    printf("main tmp buf = %s\n", tmp.get_payload());

    pbv.clear();
    for (int i = 0; i < PS; i++)
        pbv.push_back(tmp);       
    for (int i = 0; i < PS; i++)
    {
        assert(pbv[i] == tmp);
        printf("tmp buf = %s\n", pbv[i].get_payload());
    }


    //pagelist
    PageList<PageBuf> pl(pgr);
    PageList<PageBuf>::iterator it, b;

    for (int i = 0; i < PS/2; i++)
    {
        pl.push_front(pb);     
        sprintf((char *)pb.get_payload(), "list %d", i);
        printf("head %d, tail %d\n", pl.get_head(), pl.get_tail());
    }
    assert(pl.size() == PS/2);
   
    int cnt; 
    puts("forward");
    for (cnt = 0, it = pl.begin(); it != pl.end(); ++it, ++cnt)
        printf("++pl %s, prev %d, next %d\n", it->get_payload(), it->get_prev(), it->get_next());
    assert(cnt == PS/2);

    puts("backward");
    for (cnt = 0, it = --pl.end(); it != pl.end(); --it, ++cnt)
        printf("--pl %s, prev %d, next %d\n", it->get_payload(), it->get_prev(), it->get_next());
    assert(cnt == PS/2);
     
    for (cnt = 0, it = --pl.end(); it != pl.end(); ++cnt)
    {
        if (! (cnt%2))
        {
            pl.insert(it, pb);
            sprintf((char *)pb.get_payload(), "obobbo %d", cnt);
            pgr.return_page(pb);
            --it;
        }
        else if (! (cnt%5))
        {
            b = it--; 
            pl.erase(b);
        }
        else
            --it;
    }

    PageList<PageBuf>::const_iterator cit;
    puts("insert");
    for (cnt = 0, cit = pl.begin(); cit != pl.end(); ++cit, ++cnt)
        printf("++pl %s, prev %d, next %d\n", it->get_payload(), it->get_prev(), it->get_next());
    pl.clear();
    for (cnt = 0, cit = --pl.end(); cit != pl.end(); --cit, ++cnt)
        printf("--pl %s, prev %d, next %d\n", it->get_payload(), it->get_prev(), it->get_next());

    pl.push_back(pb);
    sprintf((char *)pb.get_payload(), "lolo1");
    pl.push_back(pb);
    sprintf((char *)pb.get_payload(), "lolo2");

    //PageList<PageBuf>::iterator piit;
    it = pl.begin();
    cit = it;
    //sprintf((char *)cit->get_payload(), "constnes");
    it->get_payload()[0] = 'f';

    std::list<int> fucklist;
    std::list<int> &fucklistc = fucklist;

    fucklist.push_back(3);
    fucklist.push_back(2);
    fucklist.push_back(1);

    std::list<int>::iterator lit;
    std::list<int>::const_iterator lcit;

    //lcit = fucklist.begin();
    //lit = lcit;
    
    return 0;
}

