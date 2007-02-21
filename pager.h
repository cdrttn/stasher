#ifndef _PAGER_H_
#define _PAGER_H_

#include "freecache.h"
#include "lrucache.h"
#include "fileio.h"
#include <string.h>
#include <string>

using namespace std;

namespace ST
{
    class BasicBuf;
    class PageBuf;
    class Pager;

    //Page with no default metadata. If a buffer is allocated it is freed on destruction.
    class BasicBuf
    {
    public:
        friend class Pager;

        BasicBuf(Pager &pgr); 

        virtual ~BasicBuf();

        uint32_t get_page() const { return m_page; }
        uint16_t get_pagesize() const { return m_pager.pagesize(); }
        uint16_t get_metasize() const { return m_metasize; }
        uint8_t *get_buf() { return m_buf; }
        uint8_t *get_payload() { return m_buf + m_metasize; }
        uint16_t get_payloadsize() const { return get_pagesize() - m_metasize; }

        Pager &pager() { return m_pager; }
        void clear() { memset(m_buf, 0, m_pagesize); }
        void clear_payload() { memset(get_payload(), 0, get_payloadsize()); }
        bool allocated() const { return (m_buf != NULL); }
        virtual bool validate() { return true; }
        virtual void create() {}
        void make_dirty() { if (m_lrubuf) m_lrubuf->make_dirty(); }

    protected:
        BasicBuf(Pager &pgr, uint32_t metasize); 

        Pager &m_pager;
        uint8_t *m_buf;    
        uint16_t m_metasize;

    private:
        BasicBuf(const BasicBuf &bbuf);
        BasicBuf &operator=(const BasicBuf &bbuf);
        void set_lru(LRUNode *node) 
        { 
            m_page = node->pageno; 
            m_lrubuf = node; 
            m_buf = node->buf; 
        }

        LRUNode *get_lru() 
        { 
            LRUNode *tmp = m_lrubuf; 
            m_page = 0; 
            m_buf = NULL; 
            m_lrunode = NULL; 
            return tmp;
        }

        uint32_t m_page;
        LRUNode *m_lrubuf;
    };
   
    //Page with simple metadata (type, size, next)
    class PageBuf: public BasicBuf
    {
    public:
        //basic header item offsets
        enum
        {
            HEADER_TYPE = 0,
            HEADER_SIZE = 1,
            HEADER_NEXT = 5,
            BASIC_END = 9
        };

    public:
        PageBuf(Pager &pgr): BasicBuf(pgr, BASIC_END) {}
        virtual ~PageBuf() {} 

        //basic header get/set
        uint8_t get_type() const { return get_uint8(m_buf, HEADER_TYPE); }
        void set_type(uint8_t type) { set_uint8(m_buf, HEADER_TYPE, type); }
     
        uint32_t get_size() const { return get_uint32(m_buf, HEADER_SIZE); }
        void set_size(uint32_t type) { set_uint32(m_buf, HEADER_SIZE, type); }

        uint32_t get_next() const { return get_uint32(m_buf, HEADER_NEXT); }
        void set_next(uint32_t type) { set_uint32(m_buf, HEADER_NEXT, type); }

    protected:
        PageBuf(Pager &pgr, uint16_t metasize): BasicBuf(pgr, metasize) {}
    };


    class Pager
    {
    public:
        //ENSURE -> ensure that the pages requested are actually present in the file
        //CLEAR -> same as above, but also ensure that pages are zero'd out
        enum
        {
            ALLOC_ENSURE = 1<<1,
        };

        enum
        {
            OPEN_READ = 1<<1,
            OPEN_WRITE = 1<<2,
        };
    public:
        Pager(); 
        virtual ~Pager(); 

        void open(const string &file, int flags = OPEN_WRITE, int mode = 0644, uint32_t maxcache = 512, uint16_t pagesize = 0);
        void sync();
        void close();

        void new_page(BasicBuf &buf, uint32_t page);
        void new_page(BasicBuf &buf); //allocate memory and space in the file
        void read_page(BasicBuf &buf, uint32_t page); 
        void return_page(BasicBuf &buf);
      
        bool deref_page(BasicBuf &buf, uint32_t page)
        {
            if (page)
            {
                read_page(buf, page);
                return true;
            }
            return false;
        }
       
        void read_page_dirty(BasicBuf &buf, uint32_t page)
        {
            read_page(buf, page);
            buf.make_dirty();
        }

        bool deref_page_dirty(BasicBuf &buf, uint32_t page)
        {
            if (page)
            {
                read_page_dirty(buf, page);
                return true;
            }
            return false;
        }

        // allocate count pages (bytes) on disk, return pointer
        uint32_t alloc_pages(uint32_t count, int flags = 0);

        // free count pages (bytes) starting at start
        void free_pages(uint32_t start, uint32_t count);

        void free_page(BasicBuf &buf)
        {
            uint32_t page = buf.get_page();
            return_page(buf);
            free_pages(page, 1);
        }
        
        //convert bytes to pages
        uint32_t bytes_to_pages(uint32_t bytes) const;

        string filename() const { return m_filename; }
        uint32_t length() const { return m_pagecount; }
        uint16_t pagesize() const { return m_pagesize; }
        bool writeable() const { return m_write; }

    private:
        int64_t physical(uint32_t page) { return (int64_t)m_pagesize * (int64_t)page; }
        //void clear_span(uint32_t page, uint32_t span);
        void load_free();

    private:
        uint16_t m_pagesize;
        uint32_t m_pagecount;
        bool m_write; 
        bool m_opened;
        FileIO m_io;
        FreeCache m_free;
        LRUCache m_lru;
        string m_filename;

    private:
        Pager(const Pager &);
        Pager &operator=(const Pager &);
    };
}

#endif
