#ifndef _LRUCACHE_H
#define _LRUCACHE_H

#include "ints.h"
#include "fileio.h"
#include <assert.h>
#include <map>

#define PAGEMAP map<uint32_t, LRUNode *>

namespace ST
{
    class LRUNode
    {
    public:
        friend class LRUCache;
        
        uint32_t get_pageno() const { return pageno; }
        uint8_t *get_buf() const { return buf; }
        void make_dirty() { dirty = true; }
        void set_discard() { discard = true; }
        bool anon() const { return anonymous; }
        void incref() { pinned++; }

    private:
        uint32_t pageno;
        uint8_t *buf;
        int pinned;
        bool dirty;
        bool discard;
        bool anonymous;
        bool condemned;
 
        LRUNode *next;
        LRUNode *prev;

    private:
        void decref() { assert(pinned); pinned--; }

        LRUNode(uint32_t pg, uint16_t pgsz)
        {
            pageno = pg;
            buf = new uint8_t[pgsz];
            anonymous = false;
            dirty = false;
            discard = false;
            condemned = false;
            pinned = 0;
            next = prev = NULL;
        }

        ~LRUNode()
        {
            delete buf;
        }

        void pop()
        {
            if (prev)
                prev->next = next;

            if (next)
                next->prev = prev;
        }

        LRUNode *remove()
        {
            LRUNode *tmp = next;
            pop();
            delete this;

            return tmp;
        }
        
        void push(LRUNode *&after)
        {
            if (after)
            {
                prev = after->prev;
                after->prev = this;
                if (prev)
                    prev->next = this;
            }
            next = after;
            after = this;
        }

        void push_after(LRUNode *&before)
        {
            if (before)
            {
                next = before->next;
                before->next = this;
                if (next)
                    next->prev = this;
            }
            prev = before;
            before = this;
        } 
    };

    class LRUCache
    {
    public:
        enum
        {
            DESTROY = 1,
            WIPE = 1
        };

    public:
        LRUCache()
        {
            m_head = m_tail = NULL;
            init(-1, 0, 0);
        }

        LRUCache(int fd, uint16_t pagesize, uint32_t maxcache)
        {
            m_head = m_tail = NULL;
            init(fd, pagesize, maxcache);
        }

        void init(int fd, uint16_t pagesize, uint32_t maxcache);
        void close()
        {
            sync(DESTROY);
            m_io.close();
        }
        
        ~LRUCache()
        {
            try
            {
                sync(DESTROY);
                m_io.close();
            } 
            catch (...) {}
        }

        LRUNode *new_page(uint32_t pageno);
        LRUNode *new_anon();
        LRUNode *get_page(uint32_t pageno);
        void put_page(LRUNode *node);
        void clear_extent(uint32_t offset, uint32_t span, int wipe = 0);
        void sync(int destroy = 0);
        void stats();

    private:
        int64_t physical(uint32_t page) { return (int64_t)m_pagesize * (int64_t)page; }
        LRUNode *alloc_buf(uint32_t pageno);
        void pop_buf(LRUNode *node);
        void push_buf_front(LRUNode *node);
        void push_buf_back(LRUNode *node);
        void write_buf(LRUNode *node);
        void read_buf(LRUNode *node);
         
    private:
        uint16_t m_pagesize; 
        uint32_t m_maxcache;
        uint32_t m_curcache;

        PAGEMAP m_pagemap;
        LRUNode *m_head;
        LRUNode *m_tail;
        FileIO m_io;
        int m_anon_alloc;
        uint32_t m_pagenew;
        uint32_t m_pagealloc;
        uint32_t m_pageget;
        uint32_t m_pageread;
        uint32_t m_pagewrite;
    };
}

#endif
