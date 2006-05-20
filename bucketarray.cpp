#include "pager.h"
#include "bucket.h"
#include "bucketbuf.h"
#include "bucketarray.h"
#include "hashheaderbuf.h"
#include <assert.h>

using namespace std;
using namespace ST;

BucketArray::BucketArray(HashHeaderBuf &hhb)
    :m_hhb(hhb), m_pager(hhb.pager())
{
    m_maxexp = hhb.get_maxexp();
    assert(m_maxexp > 0);

    read_heads(); 
}

void BucketArray::read_heads()
{
    PageBuf tmp(m_pager);
    PageBuf *iter;

    iter = &m_hhb;
    while (iter->get_next())
    {
        m_heads.push_back(iter->get_next());
        m_pager.read_page(tmp, iter->get_next());
        iter = &tmp;
    }
}

inline int BucketArray::chunk_length(int chunk)
{
    if (chunk < m_maxexp)
        return pow2(chunk);
    
    return pow2(m_maxexp);
}

void BucketArray::find_chunk(uint32_t index, uint32_t &chunk, uint32_t &subindex)
{
    uint32_t maxexpi = pow2(m_maxexp + 1) - 1;

    if (index < maxexpi)
    {
        chunk = log2(index + 1);
        subindex = index - pow2(chunk) + 1;
    }
    else
    {
        uint32_t pmaxexp, dist;

        index -= maxexpi;
        pmaxexp = pow2(m_maxexp);
        dist = index / pmaxexp;

        chunk = m_maxexp + 1 + dist;
        subindex = index - pmaxexp * dist;
    }
}

bool BucketArray::get(Bucket &bucket, uint32_t index)
{
    BucketBuf *bb;
    uint32_t chunk, subindex;

    assert(index < length());

    bb = bucket.get_head();
    if (!bb)
    {
        bb = new BucketBuf(m_pager, m_hhb.get_maxrec());
        bucket.set_head(bb);
    }

    find_chunk(index, chunk, subindex);
    assert(chunk < m_heads.size());

    //exceptions should be OK; caller's bucket instance owns the heap memory
    m_pager.read_page(*bb, m_heads[chunk] + subindex);
    return bb->validate();

    //XXX: bb->create() if invalid?
}

void BucketArray::resize(uint32_t size)
{
    uint32_t oldchunk, newchunk, tmp;
    PageBuf tb(m_pager);

    oldchunk = newchunk = 0;
    if (length() > 0)
        find_chunk(length()-1, oldchunk, tmp);
    if (size > 0)
        find_chunk(size-1, newchunk, tmp);

    //clear out buckets that will be removed.
    //note that they might not be subtracted below.
    if (size < length())
    {
        Bucket bucket;
        for (tmp = size; tmp < length(); tmp++)
        {
            if (get(bucket, tmp))
                bucket.clear();
        }
    }
    else if (size > 0 && m_heads.empty())
    {
        //XXX add first bucket
        assert(length() == 0);
        uint32_t ptr = m_pager.alloc_pages(1, Pager::ALLOC_CLEAR);
        m_hhb.set_next(ptr);
        m_heads.push_back(ptr);
    }

    if (newchunk > oldchunk)
    {
        // add buckets
        while (++oldchunk <= newchunk)
        {
            uint32_t span = chunk_length(oldchunk);
            uint32_t ptr = m_pager.alloc_pages(span, Pager::ALLOC_CLEAR);

            m_pager.read_page(tb, m_heads.back());
            tb.set_next(ptr);
            m_pager.write_page(tb);
            m_heads.push_back(ptr);
        }
    }
    else if (newchunk < oldchunk)
    {
        // subtract buckets 
        do
        {
            uint32_t span = chunk_length(oldchunk);

            assert(m_heads[oldchunk] == m_heads.back());
            m_pager.free_pages(m_heads.back(), span);
            m_heads.pop_back();
        } 
        while (--oldchunk > newchunk);

        m_pager.read_page(tb, m_heads.back());
        tb.set_next(0);
        m_pager.write_page(tb);
    }
    //if newchunk == oldchunk, there's enough space left in the current chunk to handle size
  
    //remove first bucket
    if (size == 0)
    {
        assert(m_heads.size() == 1);
        m_pager.free_pages(m_heads.front(), 1);
        m_heads.clear();
        m_hhb.set_next(0);
    }
    
    m_hhb.set_size(size);
    //XXX: write hhb here?
}

void BucketArray::append(Bucket &bucket)
{
    uint32_t len = length();

    resize(len + 1);
    
    BucketBuf *bb = bucket.get_head();
    if (!bb)
    {
        bb = new BucketBuf(m_pager, m_hhb.get_maxrec());
        bucket.set_head(bb);
    }

    uint32_t chunk, subindex;
    find_chunk(len, chunk, subindex);

    bb->allocate();
    bb->clear();
    bb->create();
    bb->set_page(m_heads[chunk] + subindex);
}

void BucketArray::shrink()
{
    assert(length() > 0);
    resize(length() - 1);
}

uint32_t BucketArray::length()
{
    return m_hhb.get_size();
}

