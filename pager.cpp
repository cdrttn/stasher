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


BasicBuf::BasicBuf(Pager &pgr): 
    m_pager(pgr), m_buf(NULL), m_page(0), 
    m_dirty(false), m_metasize(0)
{
}

BasicBuf::BasicBuf(Pager &pgr, uint32_t metasize): 
    m_pager(pgr), m_buf(NULL), m_page(0), 
    m_dirty(false), m_metasize(metasize)
{
}

BasicBuf::~BasicBuf()
{
    m_pager.mem_free_page(*this); 
}

Pager::Pager()
    : m_pagesize(0), m_pagecount(0), m_write(false), m_opened(false) 
{
}

Pager::~Pager() 
{ 
    catch
    {
        close(); 
    } 
    catch (...) {}
}

void Pager::open(const string &file, int flags, int mode, uint32_t maxcache, uint16_t pagesize)
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
    }

    //lru now owns fd
    m_lru.init(m_io.get_fd(), m_pagesize, maxcache);
    
    HeaderBuf header(*this);
    
    //create() sets the file page size and initial page count
    if (!strict)
        new_page(header, 0);

    read_page(header, 0);
    m_pagecount = header.get_file_pagecount();
    return_page(header);

    m_opened = true;
    m_filename = file;

    if (m_write)
        m_free.load(*this, 0);
} 


void Pager::close()
{
    if (!m_opened)
        return false;
    
    if (m_write)
    {
        HeaderBuf header(*this);
        read_page_dirty(header, 0);
        header.set_file_pagecount(m_pagecount);
        m_free.sync(*this, 0, FreeCache::DESTROY);
    }

    m_lru.close();
    m_opened = false; 
}

void Pager::sync()
{
    if (m_write)
    {
        HeaderBuf header(*this);
        read_page_dirty(header, 0);
        header.set_file_pagecount(m_pagecount);
        m_free.sync(*this, 0);
    }

    m_lru.sync();
}

//create a new dirty page
void Pager::new_page(BasicBuf &buf, uint32_t page)
{
    if (!m_write)
        throw IOException("file is readonly", m_filename);

    return_page(buf);
    buf.set_lru(m_lru.new_page(page));
    buf.clear();
    buf.create();
    buf.make_dirty();
}

//allocate space in the file and create new single page
void Pager::new_page(BasicBuf &buf)
{
    new_page(buf, alloc_pages(1));
}

void Pager::read_page(BasicBuf &buf, uint32_t page) 
{
    return_page(buf);
    buf.set_lru(m_lru.get_page(page));
}

void Pager::return_page(BasicBuf &buf)
{
    LRUNode *node;

    node = buf.get_lru();
    if (!node)
        return;

    m_lru.put_page(node);
}

uint32_t Pager::alloc_pages(uint32_t count, int flags)
{
    if (!m_write)
        throw IOException("file is readonly", m_filename);

    uint32_t offset;
    auto_ptr<FreeNode *> node(m_free.pop_free(count));

    if (!node)
    {
        offset = m_pagecount;
        m_pagecount += count;

        if (flags & ALLOC_ENSURE)
            m_io.truncate(physical(m_pagecount));
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
            m_free.push_free(node.release());
        }
        //else: used up all of freenode's space. 
    }
    
    return offset;
}

void Pager::free_pages(uint32_t start, uint32_t count)
{
    FreeNode *node;

    if (!m_write)
        throw IOException("file is readonly", m_filename);

    assert(start > 0);

    m_lru.clear_extent(start, count);
    
    //add the free range to check for merges
    node = m_free.add_free(start, count);

    //truncate and remove node from free cache and header table
    if (node->get_offset() + node->get_span() >= m_pagecount)
    {
        m_pagecount = node->get_offset();
        m_free.pop_free(node);
        delete node; 

        m_io.truncate(physical(m_pagecount));
    }
}

uint32_t Pager::bytes_to_pages(uint32_t bytes) const
{
    uint32_t pages = bytes / m_pagesize;
    if (bytes % m_pagesize)
        pages++;
    return pages;
}

