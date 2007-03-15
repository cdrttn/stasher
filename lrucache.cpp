#include "lrucache.h"
#include <assert.h>

using namespace ST;

void LRUCache::init(int fd, uint16_t pagesize, uint32_t maxcache)
{
    if (m_head)
    {
        sync(DESTROY);
        m_io.close();
    }

    m_io.set_fd(fd);
    m_pagesize = pagesize;
    m_maxcache = maxcache;

    m_curcache = 0;
    m_head = NULL;
    m_tail = NULL;

    m_pagenew = m_pagealloc = 0;
    m_pageget = m_pageread = 0;
    m_pagewrite = 0;
    m_anon_alloc = 0;
}

inline void LRUCache::pop_buf(LRUNode *node)
{
    if (node == m_head)
        m_head = node->next;
    if (node == m_tail)
        m_tail = node->prev;
    node->pop();
}

inline void LRUCache::push_buf_front(LRUNode *node)
{
    node->push(m_head);
    if (!m_tail)
        m_tail = node;
}

inline void LRUCache::push_buf_back(LRUNode *node)
{
    node->push_after(m_tail);
    if (!m_head)
        m_head = node;
}

void LRUCache::write_buf(LRUNode *node)
{
    m_pageread++;
    
    m_io.seek(physical(node->pageno));
    if (m_io.write(node->buf, m_pagesize) != m_pagesize)
        throw InvalidPageException("Short page write");

    node->dirty = false;
}

void LRUCache::read_buf(LRUNode *node)
{
    m_pagewrite++;

    m_io.seek(physical(node->pageno));
    if (m_io.read(node->buf, m_pagesize) != m_pagesize)
        throw InvalidPageException("Short page read");
}

//allocate a new page or recycle an old one
LRUNode *LRUCache::alloc_buf(uint32_t pageno)
{
    LRUNode *node = NULL;
    PAGEMAP::iterator piter;

    m_pagenew++;

    piter = m_pagemap.find(pageno);

    if (piter == m_pagemap.end())
    {
        // try to recycle memory if the cache is too large
        if (m_curcache >= m_maxcache)
        {
            node = m_tail;
            while (node && node->pinned)
                node = node->prev;

            if (node)
            {
                pop_buf(node);

                if (node->dirty)
                    write_buf(node);

                piter = m_pagemap.find(node->pageno);
                assert(piter != m_pagemap.end());
                m_pagemap.erase(piter);
                node->pageno = pageno;
            }
        }

        if (!node)
        {
            m_pagealloc++;
            node = new LRUNode(pageno, m_pagesize);
            m_curcache++;
        }
    
        m_pagemap[pageno] = node;
    }
    else
    {
        //a free page may exist in the index if it was cleared with clear_extent()
        node = piter->second;
        assert(!node->pinned && !node->dirty);
        pop_buf(node);
    }

    push_buf_front(node); 

    return node;
}

LRUNode *LRUCache::new_page(uint32_t pageno)
{
    LRUNode *node = alloc_buf(pageno);

    node->incref();

    return node;
}

LRUNode *LRUCache::new_anon()
{
    LRUNode *node = new LRUNode(0, m_pagesize);

    node->anonymous = true;
    node->incref();
    m_anon_alloc++;

    return node;
}

LRUNode *LRUCache::get_page(uint32_t pageno)
{
    LRUNode *node;
    PAGEMAP::iterator piter;

    m_pageget++;

    piter = m_pagemap.find(pageno);
    if (piter != m_pagemap.end())
    {
        node = piter->second;
        pop_buf(node);
        push_buf_front(node);
    }
    else
    {
        node = alloc_buf(pageno);
        read_buf(node);
    }

    node->incref();

    return node;
}

void LRUCache::put_page(LRUNode *node)
{
    node->decref();
    if (!node->pinned)
    {
        if (node->condemned)
        {   
            node->condemned = false;
            node->dirty = false;
        }
        if (node->anonymous)    
        {
            m_anon_alloc--;
            delete node;
        }
        else if (node->discard)
        {
            pop_buf(node);
            push_buf_back(node);
            node->discard = false; 
        }
    }
}

//mark a range that was deallocated from disk so that none of the pages will be written
//nodes that are still referenced are "condemned", so time is given for the user to remove their reference
void LRUCache::clear_extent(uint32_t offset, uint32_t span, int wipe)
{
    PAGEMAP::iterator piter;
    uint32_t ubound = offset + span;
    LRUNode *node;

    piter = m_pagemap.lower_bound(offset);
 
    while (piter != m_pagemap.end() && piter->first < ubound)
    {
        node = piter->second;
        if (node->pinned)
            node->condemned = true;
        else
            node->dirty = false;
        if (wipe)
            memset(node->buf, 0, m_pagesize);
        piter++;
    }
}

void LRUCache::sync(int destroy)
{
    LRUNode *node;
    PAGEMAP::iterator piter;

    piter = m_pagemap.begin();
    while (piter != m_pagemap.end())
    {
        node = piter->second;

        if (node->dirty && !node->pinned)
            write_buf(node);
        
        if (destroy)
        {
            PAGEMAP::iterator tmp = piter;

            assert(!node->pinned);            
            piter++;
            m_pagemap.erase(tmp);
            node->remove();
        }
        else
            piter++;
    }

    if (destroy)
    {
        assert(!m_anon_alloc);
        m_tail = m_head = NULL;
        m_curcache = 0;
    }
}

void LRUCache::stats()
{
    LRUNode *node;
    uint32_t misses = m_pageread;
    uint32_t hits = m_pageget - misses;
    int i;

    puts("");
    printf("outstanding anon allocs %d\n", m_anon_alloc);
    printf("cached %u, max %u\n", m_curcache, m_maxcache);
    printf("pagenew %u, pagealloc %u, buffers reclaimed %u\n", m_pagenew, m_pagealloc, m_pagenew - m_pagealloc);
    printf("pageget %u, pageread %u, cache misses %u\n", m_pageget, m_pageread, misses);
    if (hits + misses)
        printf("cache hitrate %.0f%%\n", (double)hits / (hits + misses) * 100);

    if (m_head || m_tail)
    {
        assert(m_head && !m_head->prev);
        assert(m_tail && !m_tail->next);

        i = 0;
        node = m_head;
        while (node)
        {
            printf("%u ", node->pageno);
            node = node->next;

            if (++i == 10)
            {
                puts("");
                i = 0;
            }
        }
        puts("");
    }
}
