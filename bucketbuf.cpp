#include "bucketbuf.h"
#include <string.h>
#include <assert.h>

using namespace std;
using namespace ST;


//return the best compatible allocation strategy SMALL, MED, LARGE
//SMALL: both key and value fit in a record
//MED: key fits in record, value overflows
//LARGE: key and value overflow
int Record::fit_me(uint32_t key_size, uint32_t value_size) const
{
    if (key_size + value_size < get_payloadsize())
        return RECORD_SMALL;

    if (key_size < get_payloadsize())
        return RECORD_MEDIUM;

    return RECORD_LARGE;
}

void Record::set_payload(const void *key, uint16_t key_size) 
{
    assert(key_size > 0);
    assert(key_size < get_payloadsize());

    set_keysize(key_size);
    memcpy(get_payload(), key, key_size);
}

void Record::set_payload(const void *key, uint16_t key_size, 
        const void *value, uint16_t value_size)
{
    assert(value_size > 0);
    assert(key_size + value_size < get_payloadsize());

    set_payload(key, key_size);
    set_valuesize(value_size);
    memcpy(get_payload() + key_size, value, value_size); 
}


bool BucketBuf::get_record(Record &rec, uint16_t index)
{
    assert(index < max_records());
    uint8_t *pbuf = get_payload() + m_recsize * index;

    rec.m_pbuf = pbuf;
    rec.m_size = m_recsize;

    //type should be > 0 for a valid record
    if (rec.get_type() != Record::RECORD_EMPTY)
        return true;

    return false;
}

void BucketBuf::remove_record(Record &rec)
{
    assert(get_size() > 0);
    assert(rec.m_size > 0);

    memset(rec.m_pbuf, 0, rec.m_size);
    set_size(get_size() - 1);
}

bool BucketBuf::insert_record(Record &rec)
{
    if (get_size() < max_records())
    {
        for (int i = 0; i < max_records(); i++)
        {
            if (!get_record(rec, i))
            {
                set_size(get_size() + 1);
                return true;
            }
        }
    }
    
    return false;
}

//count records manually (for debugging, mainly)
uint16_t BucketBuf::count_records()
{
    Record rec;
    uint16_t count = 0;

    for (int i = 0; i < max_records(); i++)
    {
        if (get_record(rec, i))
            count++;
    }

    return count;
}

//find the next filled record
bool BucketBuf::iter_next()
{
    while (m_iter_index < max_records()) 
    {
        if (get_record(m_iter_rec, m_iter_index++))
            return true;
    }

    return false;
}

//find the free filled record
bool BucketBuf::free_next()
{
    while (m_iter_index < max_records()) 
    {
        if (!get_record(m_iter_rec, m_iter_index++))
            return true;
    }

    return false;
}

