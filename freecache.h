#ifndef _FREECACHE_H_
#define _FREECACHE_H_

#include "ints.h"
#include <map>
#include <memory>

#define OFFSETS map<uint32_t, FreeNode *> 
#define FREELIST multimap<uint32_t, FreeNode *>
#define FREEITEM pair<uint32_t, FreeNode *>

using namespace std;

namespace ST
{
    class Pager;
    class FreeNode;

    class FreeCache
    {
    public:
        enum
        {
            DESTROY = 1
        };
        
    public:
        ~FreeCache();
        void load(Pager &pgr, uint32_t ptr);
        void sync(Pager &pgr, uint32_t ptr, int destroy = 0);

        //find and remove a free area of span. return NULL if no spans available.
        FreeNode *pop_free(uint32_t span);

        //same as above, but remove node
        void pop_free(FreeNode *item);

        //reinsert a modified span
        void push_free(FreeNode *item);

        //insert a new span. 
        FreeNode *add_free(uint32_t offset, uint32_t span);
       
    private:
        void freelist_remove(FreeNode *item);
        
    private:
        OFFSETS m_offsets;
        FREELIST m_freelist;
    };
}
#endif
