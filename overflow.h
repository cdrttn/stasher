#ifndef _OVERFLOW_H
#define _OVERFLOW_H

#include "overflowbuf.h"

namespace ST
{
    class Overflow
    {
    public:
        Overflow(Pager &pgr, uint32_t page = 0): 
            m_pager(pgr), m_pghead(page), m_curpage(pgr)
            {}
     
        uint32_t get_head() const { return m_pghead; }
        uint32_t init(const void *data, uint32_t size);
        uint32_t init(const buffer &data) { return init(&data[0], data.size()); }

        bool get_next_item(buffer &value);
        void add_item(const void *data, uint32_t size);
        void add_item(const buffer &value) { add_item(&value[0], value.size()); }
        void remove_item();
        void rewind();

    private:
        void check_incr(bool);
        void write_buffer(const uint8_t *data, size_t size); 
        void read_buffer(uint8_t *data, size_t size); 

    private:
        Pager &m_pager;
        uint32_t m_pghead;
        OverflowBuf m_curpage;
        uint8_t *m_curbyte;
    };
};

#endif
