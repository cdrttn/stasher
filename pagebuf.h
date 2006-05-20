#ifndef _PAGEBUF_H_
#define _PAGEBUF_H_

#include "ints.h"
#include <string>

using namespace std;

namespace ST
{
    class PageBuf
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
        PageBuf(uint16_t metasize = BASIC_END): 
            m_buf(NULL), m_page(0), m_metasize(metasize), m_pagesize(0), m_dirty(false) {}
        virtual ~PageBuf() {}

        uint32_t get_page() const { return m_page; }
        void set_page(uint32_t page) { m_page = page; }
        uint8_t *get_payload() { return m_buf + m_metasize; }
        uint8_t *get_buf() { return m_buf; }
        void set_buf(uint8_t *buf) { m_buf = buf; }
        bool is_dirty() const { return m_dirty; }
        void set_dirty(bool dirty) { m_dirty = dirty; }
        uint16_t get_pagesize() const { return m_pagesize; }
        uint16_t get_payloadsize() const { return m_pagesize - m_metasize; }
        void set_pagesize(uint16_t psz) { m_pagesize = psz; }

        bool allocated() const { return (m_buf != NULL); }
        virtual bool validate() { return true; }

        //basic header get/set
        uint8_t get_type() const { return get_uint8(m_buf, HEADER_TYPE); }
        void set_type(uint8_t type) { set_uint8(m_buf, HEADER_TYPE, type); }
     
        uint32_t get_size() const { return get_uint32(m_buf, HEADER_SIZE); }
        void set_size(uint32_t type) { set_uint32(m_buf, HEADER_SIZE, type); }

        uint32_t get_next() const { return get_uint32(m_buf, HEADER_NEXT); }
        void set_next(uint32_t type) { set_uint32(m_buf, HEADER_NEXT, type); }


    protected:
        uint8_t *m_buf;    
        uint32_t m_page;
        uint16_t m_metasize;
        uint16_t m_pagesize;
        bool m_dirty;
    };
}

#endif
