#include <assert.h>
#include "overflow.h"

using namespace std;
using namespace ST;

uint32_t Overflow::init(const void *data, uint32_t size)
{
    uint32_t pptr;
    uint32_t pages;

    pages = m_pager.bytes_to_pages(m_curpage.get_metasize() + size); 
    pptr = m_pager.alloc_pages(pages); 
    m_curpage.set_page(pptr);
    m_curpage.allocate();
    m_curpage.clear();
    m_curpage.create();
    m_curpage.set_size(size);
    m_curpage.set_freesize(pages * m_pager.pagesize() - size);
    
    m_curbyte = m_curpage.get_payload(); 
    write_buffer((uint8_t *)data, size);

    if (!m_pghead)
        m_pghead = pptr;
    
    return pptr;
}

void Overflow::add_item(const void *data, uint32_t size)
{
    uint8_t *buf;
    uint16_t freesz;
    OverflowBuf iter(m_pager);

    assert(m_pghead != 0);

    m_pager.read_page(iter, m_pghead);
    //must include size of length metadata. it is 16 bits because it is within a page
    while (iter.get_freesize() < size + 2) 
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

    iter.set_freesize(iter.get_freesize() - size - 2);
    freesz = iter.get_freesize();
    m_pager.write_page(iter);

    //last page in allocation
    m_pager.read_page(iter, iter.get_page() + iter.get_page_span() - 1);

    buf = iter.get_buf() + iter.get_pagesize() - freesz;
    set_uint16(buf, 0, size);
    buf += 2;
    memcpy(buf, data, size);
    m_pager.write_page(iter);
}

void Overflow::check_incr(bool write)
{
    if (m_curbyte > m_curpage.get_buf() + m_curpage.get_pagesize())
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
