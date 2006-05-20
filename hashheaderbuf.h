#ifndef _HASHHEADERBUF_H_
#define _HASHHEADERBUF_H_

#include "pager.h"

namespace ST
{
    class HashHeaderBuf: public PageBuf
    {
    public:
        static const uint8_t HH_MARKER = 'S';

        enum
        {
            HH_MAXREC = BASIC_END,
            HH_CAPACITY = HH_MAXREC + 2,
            HH_ITEMCOUNT = HH_CAPACITY + 2,
            HH_UNIQHASHES = HH_ITEMCOUNT + 8,
            HH_MAXEXP = HH_UNIQHASHES + 8,
            HH_END = HH_MAXEXP + 1,   
        };

    public:
        HashHeaderBuf(Pager &pgr): PageBuf(pgr, HH_END) {}

        bool validate() { return get_type() == HH_MARKER; }
        void create() { set_type(HH_MARKER); } 

        void set_maxrec(uint16_t r) { set_uint16(m_buf, HH_MAXREC, r); }
        uint16_t get_maxrec() const { return get_uint16(m_buf, HH_MAXREC); }

        void set_capacity(uint16_t c) { set_uint16(m_buf, HH_CAPACITY, c); }
        uint16_t get_capacity() const { return get_uint16(m_buf, HH_CAPACITY); }

        void set_itemcount(uint64_t ic) { set_uint64(m_buf, HH_ITEMCOUNT, ic); }
        uint64_t get_itemcount() const { return get_uint64(m_buf, HH_ITEMCOUNT); }

        void set_unique_hashes(uint64_t h) { set_uint64(m_buf, HH_UNIQHASHES, h); }
        uint64_t get_unique_hashes() const { return get_uint64(m_buf, HH_UNIQHASHES); }

        void set_maxexp(uint8_t e) { set_uint8(m_buf, HH_MAXEXP, e); }
        uint8_t get_maxexp() const { return get_uint8(m_buf, HH_MAXEXP); }
    };
}

#endif
