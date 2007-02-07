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
    RECORD_S_SIZE = 5,

    RECORD_M_KEYSIZE = 1,
    RECORD_M_SIZE = 7,

    RECORD_L_HASH32 = 1,
    RECORD_L_OVERFLOW_NEXT = 5,
    RECORD_L_END = 9
};

void Record::first_value()
{
    if (!m_recptr_save || m_type != RECORD_DUP)
        return;

    m_value = m_recptr_save + RECORD_S_SIZE + m_keysize;
}

bool Record::next_value()
{
    uint8_t *start = m_recptr_save + RECORD_S_SIZE + m_keysize;

    if (!m_recptr_save || 
            m_type != RECORD_DUP || 
            m_value > start &&
            m_value + m_valuesize >= m_recptr_save + m_size)
        return false;

    if (m_value > start)
        m_value += m_valuesize;
    m_valuesize = get_uint16(m_value, 0);
    m_value += 2;

    return true;
}

int Record::set_type(uint16_t pgsz) 
{
    uint16_t fill = pgsz / MINFILL;

    if (!m_key || !m_keysize)
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
        m_size = RECORD_M_SIZE + m_keysize; 
    }
    else
    {
        m_type = RECORD_SMALL; //Clean fit
        m_size = RECORD_S_SIZE + m_keysize + m_valuesize;
    }

    assert(m_type != RECORD_EMPTY);

    return m_type;
}


void Record::copyto(uint8_t *buf)
{
    buf[RECORD_TYPE] = m_type;
   
    // The caller is expected to allocate overflow pages prior to calling copyto()
    
    switch (m_type)
    {
        case RECORD_DUP:
        case RECORD_SMALL:
            assert(m_value && m_valuesize);

            // keysize and key
            set_uint16(buf, RECORD_S_KEYSIZE, m_keysize);
            buf += RECORD_S_KEYSIZE + 2;
            memcpy(buf, m_key, m_keysize);
            buf += m_keysize;

            // valuesize / total valuesize and value / valuelist
            set_uint16(buf, 0, m_valuesize);
            buf += 2;
            memcpy(buf, m_value, m_valuesize);
            break;

        case RECORD_MEDIUM:
            assert(m_overflow_next);

            // keysize and key
            set_uint16(buf, RECORD_M_KEYSIZE, m_keysize);
            buf += RECORD_M_KEYSIZE + 2;
            memcpy(buf, m_key, m_keysize);
            buf += m_keysize;

            // value overflow
            set_uint32(buf, 0, m_overflow_next);
            break;

        case RECORD_LARGE:
            assert(m_overflow_next);
            set_uint32(buf, RECORD_L_HASH32, m_hash32);
            set_uint32(buf, RECORD_L_OVERFLOW_NEXT, m_overflow_next);
            break;
    }
}

void Record::copyfrom(uint8_t *buf)
{
    m_type = buf[RECORD_TYPE];
    m_key = m_value = NULL;
    m_keysize = m_valuesize = 0;
    m_hash32 = m_overflow_next = 0;
  
    switch (m_type)
    {
        case RECORD_DUP:
        case RECORD_SMALL:
            //keysize and key
            m_keysize = get_uint16(buf, RECORD_S_KEYSIZE);
            buf += RECORD_S_KEYSIZE + 2;
            m_key = buf;
            buf += m_keysize;

            //valuesize and value
            m_valuesize = get_uint16(buf, 0);
            buf += 2;
            m_value = buf;

            m_size = RECORD_S_SIZE + m_keysize + m_valuesize;
            break;

        case RECORD_MEDIUM:
            //keysize and key
            m_keysize = get_uint16(buf, RECORD_M_KEYSIZE);
            buf += RECORD_M_KEYSIZE + 2;
            m_key = buf;
            buf += m_keysize;

            //value overflow ptr
            m_overflow_next = get_uint32(buf, 0);

            m_size = RECORD_M_SIZE + m_keysize; 
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

    //rec.set_type(get_pagesize());
    assert(rec.get_type() != Record::RECORD_EMPTY);

    //no room for data
    if (rec.get_size() > get_freesize())
        return false;

    //position of new data
    set_freesize(get_freesize() - rec.get_size());
    set_records(get_records() + 1);
    rptr = get_payload() + get_freesize();

    rec.copyto(rptr);

    return true;
}

void BucketBuf::remove_dup(Record &rec)
{
    uint8_t *dest, *src, *tsz;

    if (!rec.m_valuesize)
        return;
    
    rec.m_value -= 2;
    src = get_payload() + get_freesize();
    dest = src + rec.m_valuesize + 2;
    set_freesize(get_freesize() + rec.m_valuesize + 2);
    memmove(dest, src, rec.m_value - src);

    tsz = rec.m_recptr_save + RECORD_S_SIZE + rec.m_keysize;
    set_uint16(tsz, 0, get_uint16(tsz, 0) - rec.m_valuesize + 2);
    rec.m_size -= rec.m_valuesize + 2;

    rec.m_recptr_save += rec.m_valuesize + 2;
    rec.m_recptr += rec.m_valuesize + 2;
    rec.m_valuesize = 0;
}

bool BucketBuf::insert_dup(Record &rec, const void *data, uint32_t size)
{
    uint8_t *dest, *src, *rptr, *vlen;
    uint32_t vsz;
    uint16_t fill, rsize;

    fill = get_pagesize() / MINFILL; 
    assert(rec.get_type() == Record::RECORD_SMALL || rec.get_type() == Record::RECORD_DUP);
  
    //to convert a small record to a dup record, an extra 2 bytes for the first value size
    //will be required in addition to the 2 bytes for the new value size
    if (rec.get_type() == Record::RECORD_SMALL)
        rsize = size + 4;
    else
        rsize = size + 2;

    //ensure that the record will not become too large
    if (rsize > get_freesize() || rec.get_size() + rsize > fill)
        return false;

    //lift up data in the bucket
    src = get_payload() + get_freesize();
    dest = src - rsize;
    rptr = rec.m_recptr_save + rec.get_size();
    memmove(dest, src, rptr - src);
    set_freesize(get_freesize() - rsize);

    //add new value
    vsz = rec.get_valuesize();
    rec.m_recptr_save -= rsize;
    rptr = rec.m_recptr_save + rec.get_size() - vsz;
    vlen = rptr - 2;

    //convert SMALL -> DUP
    if (rec.get_type() == Record::RECORD_SMALL)
    {
        rec.m_recptr_save[0] = Record::RECORD_DUP;
        memmove(rptr + 2, rptr, vsz);
        set_uint16(rptr, 0, vsz);
        //to account for the new size metadata
        vsz += 2;
    }

    set_uint16(vlen, 0, vsz + size + 2);

    rptr += vsz;
    set_uint16(rptr, 0, size);
    rptr += 2;
    memcpy(rptr, data, size);

    rec.copyfrom(rec.m_recptr_save);
    
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
    set_records(get_records() - 1);
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

bool BucketBuf::next_record_dup(Record &rec)
{
    bool ret;

    if (!rec.m_recptr_save || 
            rec.get_type() != Record::RECORD_DUP || 
            !rec.next_value())
    {
        ret = next_record(rec);

        if (ret && rec.get_type() == Record::RECORD_DUP)
            rec.next_value();

        return ret;
    }

    return true;
}

