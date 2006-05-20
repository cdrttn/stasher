#ifndef _OVERFLOWBUF_H_
#define _OVERFLOWBUF_H_

#include "pager.h"
#include "data.h"
#include <assert.h>

namespace ST
{
    class OverflowBuf: public PageBuf
    {
    public:
        static const uint8_t OVER_MARKER = 'D';
        enum
        {
            OVER_KEY_SIZE = BASIC_END,
            OVER_VALUE_SIZE = OVER_KEY_SIZE + 4,
            OVER_END = OVER_VALUE_SIZE + 4
        };

    public:
        OverflowBuf(Pager &pgr)
            :PageBuf(pgr, OVER_END) 
        {
        }
        ~OverflowBuf() {}

        bool validate() { return get_type() == OVER_MARKER; }
        void create()
        {
            set_type(OVER_MARKER);
            set_size(0);
            set_next(0);
            set_keysize(0);
            set_valuesize(0);
        }

        uint32_t get_keysize() const { return get_uint32(m_buf, OVER_KEY_SIZE); }
        uint32_t get_valuesize() const { return get_uint32(m_buf, OVER_VALUE_SIZE); }

        void get_key(buffer &key) const;
        void get_value(buffer &value) const;
        uint32_t get_page_span() const { return m_pager.bytes_to_pages(m_metasize + get_keysize() + get_valuesize()); }

        //allocate and copy key and value
        static uint32_t alloc_payload(Pager &pgr, const void *value, uint32_t vsize);
        static uint32_t alloc_payload(Pager &pgr, const void *key, uint32_t ksize,
                const void *value, uint32_t vsize);
       
    private:
        void set_valuesize(uint32_t size) { set_uint32(m_buf, OVER_VALUE_SIZE, size); }
        void set_keysize(uint32_t size) { set_uint32(m_buf, OVER_KEY_SIZE, size); }
    };
}

#endif
