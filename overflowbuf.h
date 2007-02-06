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
            OVER_FREESIZE = BASIC_END,
            OVER_ITEM_COUNT = OVER_FREESIZE + 2,
            OVER_END = OVER_ITEM_COUNT + 2,
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
            set_extent(1);
            set_next(0);
            set_itemcount(0);
        }

        //amount of leftover space at the end of an allocation
        void set_freesize(uint16_t fs) { set_uint16(m_buf, OVER_FREESIZE, fs); }
        uint16_t get_freesize() const { return get_uint16(m_buf, OVER_FREESIZE); }

        void set_itemcount(uint16_t ic) { set_uint16(m_buf, OVER_ITEM_COUNT, ic); }
        uint16_t get_itemcount() const { return get_uint16(m_buf, OVER_ITEM_COUNT); }

        void set_extent(uint32_t ex) { set_size(ex); }
        uint32_t get_extent() const { return get_size(); }


        uint32_t get_page_span(uint32_t bigalloc) const { return m_pager.bytes_to_pages(m_metasize + bigalloc); }
    };
}

#endif
