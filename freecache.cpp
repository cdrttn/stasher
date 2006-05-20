#include "freecache.h"
#include "headerbuf.h"
#include <assert.h>

using namespace ST;
using namespace std;

FreeCache::~FreeCache()
{
    destroy();
}

void FreeCache::destroy()
{
    FREELIST::const_iterator fiter;
    HEADERS::iterator hiter;

    for (fiter = m_freelist.begin(); fiter != m_freelist.end(); fiter++)
        delete fiter->second;

    for (hiter = m_headers.begin(); hiter != m_headers.end(); hiter++)
    {
        puts("writing header ");
        HeaderBuf *buf = *hiter;
        buf->pager().write_page(*buf);
        delete buf;
    }

    m_offsets.clear();
    m_freelist.clear();
    m_headers.clear();
}

void FreeCache::add_header(HeaderBuf *buf)
{
    assert(buf->allocated());

    m_headers.push_back(buf);

    int max = buf->max_freenodes() - 1;
    while (max >= 0)
    {
        FreeNode *item = buf->scan_freenode(max--);
        if (item)
        {
            puts("FOUND item");
            pair<OFFSETS::iterator, bool> ret = m_offsets.insert(FREEITEM(item->get_offset(), item));
            assert(ret.second != false);
            m_freelist.insert(FREEITEM(item->get_span(), item));
        }
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
            puts("fl_remove found item");
            m_freelist.erase(iter);
            return;
        }
    }

    puts("fl_remove NOT FOUND item ?");
}

/*
 * Need to check for merges as such:
 * 1. find (would-be) insertion position for new item offset. 
 * 2. if new offset + new span = next higher offset in m_offsets:
 *      a. remove next higher entry from m_offsets and m_freelist
 *      b. set new span += next higher offset entry's span
 *      c. remove next higher entry's FreeNode from the header (freenode.remove())
 * 3. if next lower offset + next lower offset's span = new offset in m_offsets:
 *      a. remove next lower entry from m_offsets and m_freelist
 *      b. set new offset = next lower offset
 *      c. set new span += next lower offset's span
 *      d. remove next lower entry's FreeNode from the header (freenode.remove())
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
                other->remove();
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
                other->remove();
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
    HEADERS::iterator iter;

    assert(offset > 0 && span > 0);
    assert(m_offsets.count(offset) == 0); 
    
    for (iter = m_headers.begin(); iter != m_headers.end(); iter++)
    {
        HeaderBuf *buf = *iter;

        if (buf->get_size() < buf->max_freenodes())
        {
            printf("FC cur freenodes -> %d, max freenodes -> %d\n", buf->get_size(), buf->max_freenodes());
            FreeNode *node = buf->get_next_freenode();
            node->set_offset(offset);
            node->set_span(span);
            push_free(node);

            return node;
        }
    }

    return NULL;
}

void FreeCache::test(uint32_t offset)
{
    OFFSETS::const_iterator iter = m_offsets.lower_bound(offset);
    if (iter != m_offsets.end())
    {
        printf("lower bound -> %d\n", iter->first);
        iter++;
        if (iter != m_offsets.end())
            printf("lower bound++ -> %d\n", iter->first);
        else
            puts("NO LB++");

        iter--;
        iter--;
        if (iter != m_offsets.end())
            printf("lower bound-- -> %d\n", iter->first);
        else
            puts("NO LB--");
    } 
}




