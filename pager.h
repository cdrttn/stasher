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

        BasicBuf(Pager *pgr = NULL): m_pager(pgr), m_metasize(0), m_lrubuf(NULL) {}
        BasicBuf(const BasicBuf &bbuf): m_pager(NULL), m_lrubuf(NULL) { copy(bbuf); }
        BasicBuf &operator=(const BasicBuf &bbuf) { copy(bbuf); return *this; }
        bool operator==(const BasicBuf &r) const { return m_lrubuf == r.m_lrubuf; }        
        bool operator!=(const BasicBuf &r) const { return m_lrubuf != r.m_lrubuf; }        
        operator bool() const { return get_page() != 0; }
        bool operator!() const { return get_page() == 0; }

        virtual ~BasicBuf();

        uint16_t get_pagesize() const;
        uint16_t get_metasize() const { return m_metasize; }
        uint8_t *get_payload() { return get_buf() + m_metasize; }
        const uint8_t *get_payload() const { return get_buf() + m_metasize; }
        uint16_t get_payloadsize() const { return get_pagesize() - m_metasize; }

        Pager *pager() { return m_pager; }
        void clear() { memset(get_buf(), 0, get_pagesize()); }
        void clear_payload() { memset(get_payload(), 0, get_payloadsize()); }
        bool allocated() const { return (get_buf() != NULL); }
        void make_dirty() { if (m_lrubuf) m_lrubuf->make_dirty(); }
        void discard() { if (m_lrubuf) m_lrubuf->set_discard(); }
        bool is_anon() const { return m_lrubuf? m_lrubuf->anon() : false; }
        uint8_t *get_buf() { return m_lrubuf? m_lrubuf->get_buf() : NULL; }
        const uint8_t *get_buf() const { return m_lrubuf? m_lrubuf->get_buf() : NULL; }
        //uint8_t *get_end() { return get_buf() + get_pagesize(); }
        const uint8_t *get_end() const { return get_buf() + get_pagesize(); }
        uint32_t get_page() const { return m_lrubuf? m_lrubuf->get_pageno() : 0; }

        virtual bool validate() { return true; }
        virtual void create() {}
        virtual void release() {}

    protected:
        BasicBuf(Pager *pgr, uint32_t metasize): 
            m_pager(pgr), m_metasize(metasize), m_lrubuf(NULL) {}

        Pager *m_pager;
        uint16_t m_metasize;

    private:
        void set_lru(LRUNode *node) { m_lrubuf = node; }
        LRUNode *get_lru() 
        { 
            LRUNode *tmp = m_lrubuf; 
            m_lrubuf = NULL; 
            return tmp;
        }

        void copy(const BasicBuf &bbuf);
        
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
            HEADER_SIZE = HEADER_TYPE + 1,
            HEADER_NEXT = HEADER_SIZE + 4,
            HEADER_PREV = HEADER_NEXT + 4,
            HEADER_TAIL = HEADER_PREV + 4,
            BASIC_END = HEADER_TAIL + 4
        };

    public:
        PageBuf(Pager *pgr = NULL): BasicBuf(pgr, BASIC_END) {}
        virtual ~PageBuf() {} 

        //basic header get/set
        uint8_t get_type() const { return get_uint8(get_buf(), HEADER_TYPE); }
        void set_type(uint8_t type) { set_uint8(get_buf(), HEADER_TYPE, type); }
     
        uint32_t get_size() const { return get_uint32(get_buf(), HEADER_SIZE); }
        void set_size(uint32_t size) { set_uint32(get_buf(), HEADER_SIZE, size); }

        uint32_t get_next() const { return get_uint32(get_buf(), HEADER_NEXT); }
        void set_next(uint32_t ptr) { set_uint32(get_buf(), HEADER_NEXT, ptr); }

        uint32_t get_prev() const { return get_uint32(get_buf(), HEADER_PREV); }
        void set_prev(uint32_t ptr) { set_uint32(get_buf(), HEADER_PREV, ptr); }

        uint32_t get_tail() const { return get_uint32(get_buf(), HEADER_TAIL); }
        void set_tail(uint32_t ptr) { set_uint32(get_buf(), HEADER_TAIL, ptr); }

    protected:
        PageBuf(Pager *pgr, uint16_t metasize): BasicBuf(pgr, metasize) {}
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
        Pager(): m_pagesize(0), m_pagecount(0), m_write(false), m_opened(false) { }
        virtual ~Pager() 
        { 
            try
            { close(); } 
            catch (...) {}
        }

        void open(const string &file, int flags = OPEN_WRITE, int mode = 0644, uint32_t maxcache = 512, uint16_t pagesize = 0);
        void sync();
        void close();

        void new_page_tmp(BasicBuf &buf);
        void new_page(BasicBuf &buf, uint32_t page);
        void new_page(BasicBuf &buf) { new_page(buf, alloc_pages(1)); } 
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

        // allocate count pages on disk, return pointer
        uint32_t alloc_pages(uint32_t count, int flags = 0);

        // free count pages starting at start
        void free_pages(uint32_t start, uint32_t count);

        void free_page(BasicBuf &buf)
        {
            uint32_t page = buf.get_page();
            if (page)
            {
                buf.release();
                return_page(buf);
                free_pages(page, 1);
            } 
            else
                return_page(buf);
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
