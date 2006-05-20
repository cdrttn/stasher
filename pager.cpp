#include "pager.h"
#include "headerbuf.h"
#include "freecache.h"
#include "exceptions.h"
#include "data.h"
#include <assert.h>
#include <math.h>

//should be ok; roughly 1,000,000 entries @ 4k page size
#define MAX_FREECHUNKS 2048

using namespace std;
using namespace ST;


Pager::Pager()
    : m_pagesize(0), m_pagecount(0), m_write(false), m_opened(false) 
{
}

Pager::~Pager() 
{ 
    close(); 
}

BasicBuf::BasicBuf(Pager &pgr): 
    m_pager(pgr), m_buf(NULL), m_page(0), 
    m_pagesize(pgr.pagesize()), m_dirty(false),
    m_metasize(0)
{
}

BasicBuf::BasicBuf(Pager &pgr, uint32_t metasize): 
    m_pager(pgr), m_buf(NULL), m_page(0), 
    m_pagesize(pgr.pagesize()), m_dirty(false),  
    m_metasize(metasize)
{
}

// copy operations allocate a new buffer and copy over
// avoid copying if possible
BasicBuf::BasicBuf(const BasicBuf &bbuf):
    m_pager(bbuf.m_pager)
{
    copy(bbuf);
}

BasicBuf &BasicBuf::operator=(const BasicBuf &bbuf)
{
    assert(&m_pager == &bbuf.m_pager);
    copy(bbuf);
    return *this;
}

void BasicBuf::copy(const BasicBuf &bbuf)
{
    m_page = bbuf.m_page;
    m_pagesize = bbuf.m_pagesize;
    m_dirty = bbuf.m_dirty;
    m_metasize = bbuf.m_metasize;
    m_buf = NULL;

    if (bbuf.m_buf)
    {
        allocate();
        memcpy(m_buf, bbuf.m_buf, m_pagesize);
    }
}

BasicBuf::~BasicBuf()
{
    if (allocated())
        m_pager.mem_free_page(*this); 
}

void BasicBuf::allocate()
{
    if (!allocated())
        m_pager.mem_alloc_page(*this);
}

void Pager::open(const string &file, int flags, int mode, uint16_t pagesize)
{
    int oflags;

    if (m_opened)
        throw IOException("cannot call open twice", m_filename);
    
    if (pagesize > 0)
        m_pagesize = pagesize;
    else
        m_pagesize = FileIO::getpagesize();

    //if file exists, it should have a valid header
    bool strict = FileIO::exists(file);

    if (flags & OPEN_WRITE)
    {
        oflags = O_RDWR | O_CREAT;
        m_write = true;
    }
    else
    {
        oflags = O_RDONLY;
        m_write = false;
    }

    m_io.open(file, oflags, mode);

    HeaderBuf header(*this);
    
    if (strict)
    {
        buffer headpeak(HeaderBuf::HEADER_END);
        m_io.seek(0);
        m_io.read(&headpeak[0], headpeak.size());
        m_pagesize = HeaderBuf::check_header(headpeak);
      
        if (!m_pagesize)
        {
            m_io.close();
            throw InvalidPageException("Invalid Header", file);
        }
        
        printf("read in pagesize -> %d\n", m_pagesize);
        assert(!header.allocated());
        header.set_pagesize(m_pagesize);

        if (!read_page(header, 0) || !header.validate())
        {
            m_io.close();
            throw InvalidPageException("Invalid Header", file);
        }
    }
    else
    {
        //create() sets the file page size correctly
        header.allocate();
        header.clear();
        header.create();
        write_page(header, 0);
    }


    //load free chunks
    //FIXME: no error check
    if (m_write)
    {
        do
        {
            m_free.add_header(new HeaderBuf(header));
            printf("open freenodes -> %d, count freenodes-> %d, max freenodes -> %d\n", 
                    header.get_size(), header.count_freenodes(), header.max_freenodes());
        }
        while (header.get_next() && read_page(header, header.get_next())); 
    }

    m_opened = true;
    m_filename = file;
    m_pagecount = m_io.size() / m_pagesize;
    assert(m_io.size() == physical(m_pagecount));
} 

void Pager::close()
{
    if (m_write)
        m_free.destroy();

    m_io.close();
    m_opened = false; 
}

void Pager::mem_alloc_page(BasicBuf &buf)
{
    assert(!buf.allocated());

    uint8_t *data = new uint8_t[m_pagesize];
    buf.set_buf(data);
    buf.set_pagesize(m_pagesize);
}

void Pager::mem_free_page(BasicBuf &buf)
{
    uint8_t *data = buf.get_buf();
    if (data)
    {
        assert(buf.get_pagesize() == m_pagesize);
        delete data;
        buf.set_buf(NULL);
    }
}

//XXX: exception instead of return code?
bool Pager::read_page(BasicBuf &buf, uint32_t page) 
{
    if (!buf.allocated())
        mem_alloc_page(buf);

    assert(buf.get_pagesize() == m_pagesize);
    
    buf.set_page(page);

    m_io.seek(physical(page));
    if (m_io.read(buf.get_buf(), m_pagesize) != m_pagesize)
        return false;

    return true;
}

//XXX: exception instead of return code?
bool Pager::write_page(BasicBuf &buf, uint32_t page) 
{
    if (!m_write)
        throw IOException("file is readonly", m_filename);

    assert(buf.get_pagesize() == m_pagesize);

    m_io.seek(physical(page));
    if (m_io.write(buf.get_buf(), m_pagesize) != m_pagesize)
        return false;

    if (page >= m_pagecount)
        m_pagecount = page + 1;
    
    buf.set_page(page);
    buf.set_dirty(false);

#ifdef CHECK_PAGES
    assert(m_io.size() == physical(m_pagecount));
#endif

    return true;
}

//write a buffer ensuring page alignment
uint32_t Pager::write_buf(const void *buf, uint32_t bytes, uint32_t page, uint32_t rel)
{
    uint32_t pcount = bytes_to_pages(bytes + rel);
    uint32_t btotal = pcount * m_pagesize;
    uint16_t remainder = btotal - bytes - rel;

    if (!m_write)
        throw IOException("file is readonly", m_filename);

    m_io.seek(physical(page) + rel);

    if (m_io.write(buf, bytes) != bytes)
        return 0;

    if (remainder > 0)
    {
        buffer align(remainder, 0);
        assert(align.size() == remainder);

        if (m_io.write(&align[0], remainder) != remainder)
            return 0;
    }

    //update page count
    uint32_t xtra = page;
    if (bytes + rel > m_pagesize)
        xtra += bytes_to_pages(rel + bytes - m_pagesize);

    if (xtra >= m_pagecount)
        m_pagecount = xtra + 1;

#ifdef CHECK_PAGES
    assert(m_io.size() == physical(m_pagecount));
#endif

    return pcount;
}

//read bytes into buf starting at page
bool Pager::read_buf(void *buf, uint32_t bytes, uint32_t page, uint32_t rel)
{
    m_io.seek(physical(page) + rel);

    if (m_io.read(buf, bytes) != bytes)
        return false;

    return true;
}

//zero out a section in the file
void Pager::clear_span(uint32_t page, uint32_t span)
{
    if (!m_write)
        throw IOException("file is readonly", m_filename);

    buffer zero(m_pagesize, 0);
    assert(page > 0);
    m_io.seek(physical(page));
   
    while (span-- >= 0)
        m_io.write(&zero[0], m_pagesize);
}

uint32_t Pager::alloc_pages(uint32_t count, int flags)
{
    if (!m_write)
        throw IOException("file is readonly", m_filename);

    uint32_t offset;
    FreeNode *node = m_free.pop_free(count);

    if (!node)
    {
        offset = m_pagecount;

        if (flags & ALLOC_ENSURE)
        {
            m_pagecount += count;
            m_io.truncate(physical(m_pagecount));
        }
    }
    else
    {
        offset = node->get_offset();

        assert(node->get_span() >= count);
        if (node->get_span() > count)
        {
            //move the remaining free space down
            node->set_offset(offset + count);
            node->set_span(node->get_span() - count);
            m_free.push_free(node);
        }
        else
            node->remove(); //used up all of freenode's space, so delete it

        if (flags & ALLOC_CLEAR)
            clear_span(offset, count);

        //XXX: sync header table?
    }

#ifdef CHECK_PAGES
    assert(m_io.size() == physical(m_pagecount));
#endif
    
    return offset;
}

uint32_t Pager::alloc_bytes(uint32_t bytes, int flags)
{
    return alloc_pages(bytes_to_pages(bytes), flags);
}

void Pager::free_pages(uint32_t start, uint32_t count)
{
    if (!m_write)
        throw IOException("file is readonly", m_filename);

    assert(start > 0);

    //add the free range to check for merges
    FreeNode *node = m_free.add_free(start, count);

    //FIXME: free chunks are never removed...
    //XXX: should freecache append free chunks instead of pager?
    if (!node)
    {
        uint32_t ptr = alloc_pages(1);
        HeaderBuf header(*this);
        HeaderBuf *last = m_free.headers().back();
       
        //If the file becomes excessively large and fragmented,
        //free pages will be ignored, unfortunetly.
        if (m_free.headers().size() >= MAX_FREECHUNKS)
            return;

        assert(last->get_next() == 0);

        header.allocate();
        header.clear();
        header.create();
        write_page(header, ptr);
        last->set_next(ptr);
        write_page(*last);

        m_free.add_header(new HeaderBuf(header));
        node = m_free.add_free(start, count);
        assert(node != NULL);

        printf("tacked on new freechunk -> %d total\n", m_free.headers().size());
    }

    //truncate and remove node from free cache and header table
    if (node->get_offset() + node->get_span() >= m_pagecount)
    {
        m_pagecount = node->get_offset();
        m_free.pop_free(node);
        node->remove();

        m_io.truncate(physical(m_pagecount));
    }

    //XXX: sync header table?
    
#ifdef CHECK_PAGES
    assert(m_io.size() == physical(m_pagecount));
#endif
}

void Pager::free_bytes(uint32_t start, uint32_t bytes)
{
    free_pages(start, bytes_to_pages(bytes));
}

uint32_t Pager::bytes_to_pages(uint32_t bytes) const
{
    double pages = (double)bytes / (double)m_pagesize;

    return (uint32_t)ceil(pages);
}

