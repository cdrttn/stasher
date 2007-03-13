#include "freecache.h"
#include "headerbuf.h"
#include <exception>
#include <assert.h>

using namespace ST;
using namespace std;

FreeCache::~FreeCache()
{
    //XXX: meh fix this shit
    //destroy();
}

void FreeCache::load(Pager &pgr, uint32_t ptr)
{
    uint16_t i;
    uint32_t next, offset, span;
    HeaderBuf head;

    pgr.read_page_dirty(head, ptr);

    do
    {
        for (i = 0; i < head.get_size(); i++)
        {
            head.get_freenode(i, offset, span);
            add_free(offset, span);
        }

        next = head.get_next();
        if (head.get_page() != ptr)
            pgr.free_page(head);
        else
        {
            head.set_size(0);
            head.set_next(0);
            head.clear_payload();
        }
    }
    while (pgr.deref_page(head, next));
}

//XXX: if an exception is thrown during this function the file and cache are corrupted
void FreeCache::sync(Pager &pgr, uint32_t ptr, int destroy)
{
    size_t len;
    uint16_t maxfn;
    uint32_t chunk = 0, clen;
    HeaderBuf head;
    FREELIST::const_iterator fiter;

    //sync freelist to file using available pages. allocate needed pages first
    pgr.read_page_dirty(head, ptr);
    assert(head.get_size() == 0);
    len = m_freelist.size();
    maxfn = head.max_freenodes();

    if (len > maxfn)
    {
        len -= maxfn;
        clen = len / maxfn;
        if (len % maxfn)
            clen++;

        //this might allocate an unneeded page if the allocation causes removal of a freenode
        //no biggie, though
        chunk = pgr.alloc_pages(clen);
        printf("chunk %d, clen %d\n", chunk, clen);
    }

    for (fiter = m_freelist.begin(); fiter != m_freelist.end(); fiter++)
    {
        if (head.get_size() == head.max_freenodes())
        {
            assert(chunk);
            head.set_next(chunk);
            printf("%u set_next %u\n", head.get_page(), chunk);
            pgr.new_page(head, chunk++);
        }
       
        head.add_freenode(fiter->second);
        
        if (destroy)
            delete fiter->second;
    }

    if (destroy)
    {
        m_offsets.clear();
        m_freelist.clear();
    }
}

FreeNode *FreeCache::pop_free(uint32_t span)
{
    assert(span > 0);

    FreeNode *ret = NULL;
    FREELIST::iterator iter = m_freelist.lower_bound(span);

    if (iter != m_freelist.end())
    {
        ret = iter->second;
        m_freelist.erase(iter);
        m_offsets.erase(ret->get_offset());
    }

    return ret;
}

void FreeCache::pop_free(FreeNode *item)
{
    freelist_remove(item);
    m_offsets.erase(item->get_offset());
}

void FreeCache::freelist_remove(FreeNode *item)
{
    FREELIST::iterator iter;
    pair<FREELIST::iterator, FREELIST::iterator> ret;

    ret = m_freelist.equal_range(item->get_span());
    for (iter = ret.first; iter != ret.second; iter++)
    {
        if (iter->second == item)
        {
            m_freelist.erase(iter);
            return;
        }
    }

    //shouldn't reach this
    terminate();
}

/*
 * Need to check for merges as such:
 * 1. find (would-be) insertion position for new item offset. 
 * 2. if new offset + new span = next higher offset in m_offsets:
 *      a. remove next higher entry from m_offsets and m_freelist
 *      b. set new span += next higher offset entry's span
 *      c. remove next higher entry's FreeNode 
 * 3. if next lower offset + next lower offset's span = new offset in m_offsets:
 *      a. remove next lower entry from m_offsets and m_freelist
 *      b. set new offset = next lower offset
 *      c. set new span += next lower offset's span
 *      d. remove next lower entry's FreeNode 
 * 4. insert new item FreeNode into m_offsets and m_freelist
 */
void FreeCache::push_free(FreeNode *item)
{
    assert(item);
    assert(item->get_offset() > 0 && item->get_span() > 0);

    //OFFSETS::iterator oiter;
    uint32_t offset, span;
    offset = item->get_offset();
    span = item->get_span();

    if (!m_offsets.empty())
    {
        FreeNode *other;
        bool has_before = false;
        OFFSETS::iterator after, before;

        //#1 insertion point
        after = m_offsets.lower_bound(offset);
        if (after != m_offsets.begin())
        {
            before = after;
            before--;
            has_before = true;
        }

        //#2 check after
        if (after != m_offsets.end())
        {
            //lower_bound should yield the item at the first greater offset
            assert(after->first > offset);

            other = after->second;
            assert(offset + span <= other->get_offset()); //overlapping freenode entries lead to corruption
            if (offset + span == other->get_offset())
            {
                m_offsets.erase(after);
                freelist_remove(other);
                span += other->get_span();
                delete other;
            }
        }

        //#3 check before
        if (has_before)
        {
            assert(before->first < offset);

            other = before->second;
            assert(other->get_offset() + other->get_span() <= offset); //overlapping freenode entries lead to corruption
            if (other->get_offset() + other->get_span() == offset)
            {
                m_offsets.erase(before);
                freelist_remove(other);
                span += other->get_span();
                offset = other->get_offset();
                delete other;
            }
        }
    }

    item->set_span(span);
    item->set_offset(offset);

    //#4 insert
    m_offsets.insert(FREEITEM(item->get_offset(), item));
    m_freelist.insert(FREEITEM(item->get_span(), item));
}

FreeNode *FreeCache::add_free(uint32_t offset, uint32_t span)
{
    FreeNode *node;

    assert(offset > 0 && span > 0);
    assert(m_offsets.count(offset) == 0); 

    node = new FreeNode(offset, span);
    push_free(node);
    
    return node;
}
