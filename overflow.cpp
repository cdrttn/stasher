#include <assert.h>
#include "overflow.h"

using namespace std;
using namespace ST;

uint32_t Overflow::init(const void *data, uint32_t size)
{
    uint32_t pptr;
    uint32_t pages;

    pages = m_curpage.get_page_span(size + 4);
    pptr = m_pager.alloc_pages(pages); 

    m_curpage.allocate();
    m_curpage.clear();
    m_curpage.create();
    m_curpage.set_freesize(pages * m_pager.pagesize() - size - 4 - m_curpage.get_metasize());
    m_curpage.set_page(pptr);
    m_curpage.set_itemcount(1);
    m_curpage.set_extent(pages);

    m_curbyte = m_curpage.get_payload() + m_curpage.get_freesize(); 
    set_uint32(m_curbyte, 0, size);
    m_curbyte += 4;
    write_buffer((uint8_t *)data, size);

    if (!m_pghead)
        m_pghead = pptr;
    
    return pptr;
}

void Overflow::remove_item()
{
    uint8_t *start, *dest; 
    uint32_t osize;

    if (!m_lastbyte || !m_curitem)
        return;

    if (m_curpage.get_page() != m_toppage)
    {
        uint32_t pages, pagealign;
        uint32_t top;

        
        m_pager.read_page(m_curpage, m_toppage);
        pages = m_curpage.get_extent();
        pagealign = pages * m_pager.pagesize();
        top = pagealign - m_lastsize - 4 - m_curpage.get_metasize(); //original freesize
        m_lastbyte = m_curpage.get_payload() + top;
        m_lastsize = m_curpage.get_payloadsize() - top - 4; //bottom
        m_curpage.set_extent(1);
        m_pager.free_pages(m_toppage + 1, pages - 1);
    }

    m_curpage.set_itemcount(m_curpage.get_itemcount() - 1);
    
    //pop out this link unless this is the head
    if (m_curpage.get_itemcount() == 0 && m_prevpage)
    {
        m_pager.free_pages(m_toppage, 1);
        m_pager.read_page(m_curpage, m_prevpage);
        m_curpage.set_next(m_nextpage);
        m_toppage = m_prevpage;
        m_pager.write_page(m_curpage);

        if (m_nextpage)
            start_iter(m_nextpage); 
    }
    else 
    {
        //move down, as with bucket
        osize = m_curpage.get_freesize();
        m_curpage.set_freesize(osize + m_lastsize + 4);
        dest = m_curpage.get_payload() + m_curpage.get_freesize();
        start = m_curpage.get_payload() + osize;

        if (start != m_lastbyte)
            memmove(dest, start, m_lastbyte - start);

        memset(start, 0, m_lastsize + 4); 
        m_pager.write_page(m_curpage);
    }
}

void Overflow::start_iter(uint32_t page)
{
    m_pager.read_page(m_curpage, page);
    m_maxitems = m_curpage.get_itemcount();
    if (m_toppage)
        m_prevpage = m_toppage;
    else
        m_prevpage = 0;

    m_toppage = page;
    m_nextpage = m_curpage.get_next();
    m_curitem = 0;
    m_lastbyte = NULL;
    m_lastsize = 0;
    m_curbyte = m_curpage.get_payload() + m_curpage.get_freesize();
}

void Overflow::rewind()
{
    if (m_pghead)
    {
        m_toppage = 0;
        start_iter(m_pghead);
    }
}

bool Overflow::get_next_item(buffer &value)
{
    uint32_t itemsz;

    if (!m_pghead)
        return false;
    
    if (m_curitem >= m_maxitems) 
    {
        if (m_nextpage)
            start_iter(m_nextpage);
        else
            return false;
    }

    m_lastbyte = m_curbyte;
    itemsz = get_uint32(m_curbyte, 0);
    m_curbyte += 4;

    value.resize(itemsz);
    read_buffer(&value[0], itemsz);
    
    m_curitem++;
    m_lastsize = itemsz;
    
    return true;
}

void Overflow::add_item(const void *data, uint32_t size)
{
    uint8_t *buf;
    OverflowBuf iter(m_pager);

    if (!m_pghead)
    {
        init(data, size);
        return;
    }

    m_pager.read_page(iter, m_pghead);
    //must include size of length metadata, 32 bits 
    while (iter.get_freesize() < size + 4) 
    {
        if (!iter.get_next())
        {
            iter.set_next(init(data, size)); 
            m_pager.write_page(iter);
            return;
        }
        else
            m_pager.read_page(iter, iter.get_next());
    }

    iter.set_itemcount(iter.get_itemcount() + 1);
    iter.set_freesize(iter.get_freesize() - size - 4);

    buf = iter.get_payload() + iter.get_freesize();
    set_uint32(buf, 0, size);
    buf += 4;
    memcpy(buf, data, size);
    m_pager.write_page(iter);
}

void Overflow::clear()
{
    uint32_t page;
    OverflowBuf iter(m_pager);

    if (!m_pghead)
        return;

    page = m_pghead;

    do
    {
        m_pager.read_page(iter, page);
        m_pager.free_pages(page, iter.get_extent());
    } while ((page = iter.get_next()) != 0);

    m_pghead = 0;
}

void Overflow::check_incr(bool write)
{
    if (m_curbyte >= m_curpage.get_buf() + m_curpage.get_pagesize())
    {
        if (write)
            m_pager.write_page(m_curpage);

        m_curpage.clear();
        m_pager.read_page(m_curpage, m_curpage.get_page() + 1);
        m_curbyte = m_curpage.get_buf();
    }
}

void Overflow::write_buffer(const uint8_t *data, size_t size)
{
    while (size--)
    {
        check_incr(true);
        *m_curbyte++ = *data++;
    }

    m_pager.write_page(m_curpage);
}

void Overflow::read_buffer(uint8_t *data, size_t size)
{
    while (size--)
    {
        check_incr(false);
        *data++ = *m_curbyte++;
    }
}
