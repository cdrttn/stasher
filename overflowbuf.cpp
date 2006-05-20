#include "overflowbuf.h"
#include <math.h>

using namespace std;
using namespace ST;

static uint32_t dump_header(Pager &pgr, uint32_t page, uint32_t ksize, uint32_t vsize)
{
    buffer vbuf(OverflowBuf::OVER_END);
    uint8_t *buf = &vbuf[0];

    set_uint8(buf, PageBuf::HEADER_TYPE, OverflowBuf::OVER_MARKER);
    set_uint32(buf, PageBuf::HEADER_SIZE, ksize + vsize);
    set_uint32(buf, PageBuf::HEADER_NEXT, 0);
    set_uint32(buf, OverflowBuf::OVER_KEY_SIZE, ksize);
    set_uint32(buf, OverflowBuf::OVER_VALUE_SIZE, vsize);
   
    pgr.write_buf(buf, OverflowBuf::OVER_END, page);

    return OverflowBuf::OVER_END;
}

uint32_t OverflowBuf::alloc_payload(Pager &pgr, const void *value, uint32_t vsize)
{
    uint32_t totsize = OVER_END + vsize;
    uint32_t ptr = pgr.alloc_bytes(totsize);

    uint32_t start = dump_header(pgr, ptr, 0, vsize);
    pgr.write_buf(value, vsize, ptr, start);

    return ptr;
}

uint32_t OverflowBuf::alloc_payload(Pager &pgr, const void *key, uint32_t ksize,
    const void *value, uint32_t vsize)
{
    uint32_t totsize = OVER_END + vsize + ksize;
    uint32_t ptr = pgr.alloc_bytes(totsize);

    uint32_t start = dump_header(pgr, ptr, ksize, vsize);
    pgr.write_buf(key, ksize, ptr, start);
    pgr.write_buf(value, vsize, ptr, start + ksize);

    return ptr;
}

void OverflowBuf::get_key(buffer &key) const
{
    if (get_keysize() == 0)
        return;

    key.resize(get_keysize());
    m_pager.read_buf(&key[0], get_keysize(), get_page(), m_metasize);
}

void OverflowBuf::get_value(buffer &value) const
{
    if (get_valuesize() == 0)
        return;

    value.resize(get_valuesize());
    m_pager.read_buf(&value[0], get_valuesize(), get_page(), m_metasize + get_keysize());
}

