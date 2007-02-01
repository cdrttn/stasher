#include "bucketbuf.h"
#include <string.h>
#include <assert.h>

using namespace std;
using namespace ST;

#define MINFILL 4
enum
{
    RECORD_TYPE = 0,

    RECORD_S_KEYSIZE = 1,
    RECORD_S_VALUESIZE = 3,
    RECORD_S_END = 5,

    RECORD_M_KEYSIZE = 1,
    RECORD_M_OVERFLOW_NEXT = 3,
    RECORD_M_END = 7,

    RECORD_L_HASH32 = 1,
    RECORD_L_OVERFLOW_NEXT = 5,
    RECORD_L_END = 9
};

int Record::set_type(uint16_t pgsz) 
{
    uint16_t fill = pgsz / MINFILL;

    if (!m_key || !m_value || !m_keysize || !m_valuesize)
    {
        m_type = RECORD_EMPTY;
        m_size = 0;
    }
    else if (m_keysize > fill) //Key and value overflow 
    {
        m_type = RECORD_LARGE;
        m_size = RECORD_L_END; //Just metadata stored
    }
    else if (m_valuesize > fill) //Value overflows
    {
        m_type = RECORD_MEDIUM;
        m_size = RECORD_M_END + m_keysize; 
    }
    else
    {
        m_type = RECORD_SMALL; //Clean fit
        m_size = RECORD_S_END + m_keysize + m_valuesize;
    }

    return m_type;
}


void Record::copyto(uint8_t *buf)
{
    buf[RECORD_TYPE] = m_type;
   
    // The caller is expected to allocate overflow pages prior to calling copyto()
    
    switch (m_type)
    {
        case RECORD_SMALL:
            set_uint16(buf, RECORD_S_KEYSIZE, m_keysize);
            set_uint16(buf, RECORD_S_VALUESIZE, m_valuesize);
            buf += RECORD_S_END;
            memcpy(buf, m_key, m_keysize);
            buf += m_keysize;
            memcpy(buf, m_value, m_valuesize);
            break;

        case RECORD_MEDIUM:
            assert(m_overflow_next);
            set_uint16(buf, RECORD_M_KEYSIZE, m_keysize);
            set_uint32(buf, RECORD_M_OVERFLOW_NEXT, m_overflow_next);
            buf += RECORD_M_END;
            memcpy(buf, m_key, m_keysize);
            break;

        case RECORD_LARGE:
            assert(m_overflow_next);
            set_uint32(buf, RECORD_L_HASH32, m_hash32);
            set_uint32(buf, RECORD_L_OVERFLOW_NEXT, m_overflow_next);
            break;
    }

    //m_recptr = buf;
}

void Record::copyfrom(uint8_t *buf)
{
    m_type = buf[RECORD_TYPE];
    m_key = m_value = NULL;
    m_keysize = m_valuesize = 0;
    m_hash32 = m_overflow_next = 0;
    
    switch (m_type)
    {
        case RECORD_SMALL:
            m_keysize = get_uint16(buf, RECORD_S_KEYSIZE);
            m_valuesize = get_uint16(buf, RECORD_S_VALUESIZE);
            m_key = buf + RECORD_S_END;
            m_value = m_key + m_keysize;
            m_size = RECORD_S_END + m_keysize + m_valuesize;
            break;

        case RECORD_MEDIUM:
            m_keysize = get_uint16(buf, RECORD_M_KEYSIZE);
            m_overflow_next = get_uint32(buf, RECORD_M_OVERFLOW_NEXT);
            m_key = buf + RECORD_M_END;
            m_size = RECORD_M_END + m_keysize; 
            break;

        case RECORD_LARGE:
            m_hash32 = get_uint32(buf, RECORD_L_HASH32);
            m_overflow_next = get_uint32(buf, RECORD_L_OVERFLOW_NEXT);
            m_size = RECORD_L_END; 
            break;
    }
}

uint16_t BucketBuf::count_records()
{
    Record rec;
    uint16_t count;
    
    count = 0;
    first_record(rec);
    
    while (next_record(rec))
        count++;

    return count;
}

bool BucketBuf::insert_record(Record &rec)
{
    uint8_t *rptr;

    rec.set_type(get_pagesize());
    assert(rec.get_type() != Record::RECORD_EMPTY);

    //no room for data
    if (rec.get_size() > get_freesize())
        return false;

    //position of new data
    set_freesize(get_freesize() - rec.get_size());
    rptr = get_payload() + get_freesize();

    rec.copyto(rptr);

    return true;
}

void BucketBuf::remove_record(Record &rec)
{
    uint8_t *rptr, *start, *dest;
    uint16_t osize;

    rptr = rec.m_recptr_save;
    if (!rptr)
        return;

    assert(rptr >= get_payload() && rptr < get_payload() + get_payloadsize());

    rec.copyfrom(rptr);
    osize = get_freesize();
    set_freesize(osize + rec.get_size());
    dest = get_payload() + get_freesize();
    start = get_payload() + osize;

    //no need to move the first element
    if (get_payload() + osize != rptr) 
        memmove(dest, start, rptr - start);

    memset(start, 0, rec.get_size());
}

void BucketBuf::first_record(Record &rec)
{
    rec.m_recptr = get_payload() + get_freesize();
}

bool BucketBuf::next_record(Record &rec)
{
    if (!rec.m_recptr || rec.m_recptr >= get_payload() + get_payloadsize())
        return false;
    
    rec.copyfrom(rec.m_recptr);

    rec.m_recptr_save = rec.m_recptr;
    rec.m_recptr += rec.get_size();

    return true;
}
