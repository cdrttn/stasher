#ifndef _FREECACHE_H_
#define _FREECACHE_H_

#include "ints.h"
#include <map>
#include <vector>

#define HEADERS vector<HeaderBuf *> 
#define OFFSETS map<uint32_t, FreeNode *> 
#define FREELIST multimap<uint32_t, FreeNode *>
#define FREEITEM pair<uint32_t, FreeNode *>

using namespace std;

namespace ST
{
    class HeaderBuf;
    class FreeNode;

    class FreeCache
    {
    public:
        ~FreeCache();
        void destroy();

        //return true if all available header pages are full
        //bool is_exhausted();

        //load freenodes from buf 
        void add_header(HeaderBuf *buf);
        HEADERS &headers() { return m_headers; }

        //find and remove a free area of span. return NULL if no spans available.
        FreeNode *pop_free(uint32_t span);

        //same as above, but remove node
        void pop_free(FreeNode *item);

        //reinsert a modified span
        void push_free(FreeNode *item);

        //insert a new span. return null if underlying pages are full
        FreeNode *add_free(uint32_t offset, uint32_t span);

        void test(uint32_t offset);
       
    private:
        void freelist_remove(FreeNode *item);
        
    private:
        OFFSETS m_offsets;
        FREELIST m_freelist;
        HEADERS m_headers;
    };
}
#endif
