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

BucketArray::~BucketArray()
{
    if (!m_pager.writeable())
        return;

    size_t i;
    for (i = 0; i < m_headbufs.size(); i++)
    {
        HashHeaderBuf *hhb = m_headbufs[i];

        m_pager.write_page(*hhb);
        if (hhb != &m_hhb)
            delete hhb;
    }
}

void BucketArray::read_heads()
{
    HashHeaderBuf tmp(m_pager);
    HashHeaderBuf *iter;

    iter = &m_hhb;
    
    while (1)
    {
        assert(iter == &m_hhb || iter->get_chunkcount() > 0);

        uint16_t i;
        for (i = 0; i < iter->get_chunkcount(); i++)
            m_heads.push_back(iter->get_chunk(i));

        
        //don't clone the main header to keep it synched
        if (iter == &m_hhb)
            m_headbufs.push_back(iter);
        else
            m_headbufs.push_back(new HashHeaderBuf(*iter));

        if (!iter->get_next())
            break;

        m_pager.read_page(tmp, iter->get_next());
        iter = &tmp;
    }

    printf("heads(%d), headbufs(%d)\n", m_heads.size(), m_headbufs.size());
    
    //XXX: should check for errors (iter->validate)
    //
    //XXX: should verify that the number of heads read in 
    //     is suitable for # of buckets (hhb.get_size())
}

void BucketArray::append_head(uint32_t offset) 
{
    assert(!m_headbufs.empty());
    HashHeaderBuf *back = m_headbufs.back();

    m_heads.push_back(offset);

    if (back->get_chunkcount() < back->max_chunks())
        back->append_chunk(offset);
    else
    {
        uint32_t ptr = m_pager.alloc_pages(1);
        HashHeaderBuf *hhb = new HashHeaderBuf(m_pager);
        m_headbufs.push_back(hhb);

        hhb->allocate();
        hhb->clear();
        hhb->create();
        hhb->append_chunk(offset);
        m_pager.write_page(*hhb, ptr);
        back->set_next(ptr);
    }

    m_pager.write_page(*back);
}

void BucketArray::shrink_head()
{
    assert(!m_headbufs.empty());
    HashHeaderBuf *back = m_headbufs.back();

    assert(back->get_chunkcount() > 0);
    assert(back->get_chunk(back->get_chunkcount() - 1) == m_heads.back());
    back->shrink_chunk();
    m_heads.pop_back();

    if (back->get_chunkcount() == 0 && m_headbufs.size() > 1)
    {
        m_pager.free_pages(back->get_page(), 1);
        m_headbufs.pop_back();
        if (back != &m_hhb)
            delete back;
       
        back = m_headbufs.back();
        back->set_next(0);
        m_pager.write_page(*back);
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

void BucketArray::get(Bucket &bucket, uint32_t index)
{
    BucketBuf *bb;
    uint32_t chunk, subindex;

    assert(index < length());

    bb = bucket.get_head();
    if (!bb)
    {
        bb = new BucketBuf(m_pager);
        bucket.set_head(bb);
    }

    find_chunk(index, chunk, subindex);
    assert(chunk < m_heads.size());

    //exceptions should be OK; caller's bucket instance owns the heap memory
    m_pager.read_page(*bb, m_heads[chunk] + subindex);
    if (!bb->validate()) 
        bb->create(); 
}

//FIXME: 
//bleh, linking the bucket chunks together causes problems for get()
//and append()... bug is difficult to explain. Briefly, if a bucket is gotten,
//then a new bucket appended, the bucket gotten may not have the new link
//data generated due to the subsequent call to append(). The link structure is
//corrupted when the unsync'd gotten bucket is written out.
//
//I'm thinking it would be best to dump the linked list and store bucket heads
//in HashHeaderBuf ala HeaderBuf/FreeNode. It would be faster for loading, too.
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
            get(bucket, tmp);
            bucket.clear();
        }
    }
    else if (size > 0 && m_heads.empty())
    {
        //XXX add first bucket
        assert(length() == 0);
        uint32_t ptr = m_pager.alloc_pages(1, Pager::ALLOC_CLEAR);
        append_head(ptr);
    }

    if (newchunk > oldchunk)
    {
        // add buckets
        while (++oldchunk <= newchunk)
        {
            uint32_t span = chunk_length(oldchunk);
            uint32_t ptr = m_pager.alloc_pages(span, Pager::ALLOC_CLEAR);

            append_head(ptr);
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
            shrink_head();
        } 
        while (--oldchunk > newchunk);
    }
    //if newchunk == oldchunk, there's enough space left in the current chunk to handle size
  
    //remove first bucket
    if (size == 0)
    {
        assert(m_heads.size() == 1);
        assert(m_headbufs.size() == 1);
        m_pager.free_pages(m_heads.front(), 1);
        shrink_head();
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
        bb = new BucketBuf(m_pager);
        bucket.set_head(bb);
    }

    uint32_t chunk, subindex;
    uint32_t ptr;
    find_chunk(len, chunk, subindex);

    ptr = m_heads[chunk] + subindex; 
    
    bb->allocate();
    bb->clear();
    bb->create();
    bb->set_page(ptr);
    //bb->set_next(m_heads[chunk]);
}

void BucketArray::last(Bucket &bucket)
{
    assert(length() > 0);
    get(bucket, length() - 1);
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

