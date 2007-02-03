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
            OVER_ITEM_COUNT = BASIC_END,
            OVER_FREE_SIZE = OVER_ITEM_COUNT + 4,
            OVER_END = OVER_FREE_SIZE + 2,
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
            set_freesize(0);
            set_next(0);
            set_itemcount(0);
        }

        void set_itemcount(uint32_t ic) { set_uint32(m_buf, OVER_ITEM_COUNT, ic); }
        uint32_t get_itemcount() const { return get_uint32(m_buf, OVER_ITEM_COUNT); }

        //amount of leftover space at the end of an allocation
        void set_freesize(uint32_t fs) { set_uint16(m_buf, OVER_FREE_SIZE, fs); }
        uint16_t get_freesize() const { return get_uint16(m_buf, OVER_FREE_SIZE); }

        uint32_t get_page_span() const { return m_pager.bytes_to_pages(m_metasize + get_size()); }
    };
}

#endif
