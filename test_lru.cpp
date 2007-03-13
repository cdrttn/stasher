#include "lrucache.h"
#include <stdio.h>

using namespace ST;

int main(int argc, char **argv)
{
    FileIO fp;
    LRUCache lru;
    int maxcache = 4000;
    
    if (argc < 2)
    {
        puts("lru db [maxcache]");
        return 0;
    }
  
    if (argc >= 3)
    {
        int tmp = atoi(argv[2]);
        if (tmp)
            maxcache = tmp;
    }
    
    fp.open(argv[1], O_RDWR | O_CREAT);
    lru.init(fp.get_fd(), 4096, maxcache);
   
    int i;
    for (i = 0; i < 1500; i++)
    {
        LRUNode *node = lru.new_page(i);
        sprintf((char*)node->get_buf(), "what the fux, %d", i);
        node->make_dirty();
        lru.put_page(node);
    }
   
    for (i = 0; i < 1500; i++)
    {
        if (!(i%2))
        {
            LRUNode *node = lru.get_page(i);
            puts((char*)node->get_buf());
            node->set_discard();
            lru.put_page(node);
        }
    }

    lru.put_page(lru.new_anon());
   
    lru.stats();
   
    lru.close();
    fp.open(argv[1], O_RDONLY);
    lru.init(fp.get_fd(), 4096, maxcache);

    for (i = 0; i < 15; i++)
    {
        LRUNode *node = lru.get_page(i);
        puts((char*)node->get_buf());
        lru.put_page(node);
    }

    
    return 0;
}
