#ifndef _BUCKETBUF_H_
#define _BUCKETBUF_H_

#include "pager.h"
#include <assert.h>

namespace ST
{
    class Record;
    class BucketBuf;

    class Record
    {
    public:
        friend class BucketBuf;
        enum
        {
            //small -> key and value fit within bucket record
            //medium -> key fits, value overflows
            //large -> key and value overflow
            RECORD_EMPTY = 0,
            RECORD_SMALL,
            RECORD_MEDIUM,
            RECORD_LARGE
        };

        enum
        {
            RECORD_HASH32 = 0,
            RECORD_OVERFLOW_NEXT = 4,
            RECORD_KEY_SIZE = 8,
            RECORD_VALUE_SIZE = 10,
            RECORD_END = 12
        };

    public:
        Record(): m_pbuf(NULL), m_size(0) {}
        Record &operator=(const Record &rec)
        {
            if (m_pbuf && rec.m_pbuf)
            {
                assert(m_size == rec.m_size);
                memcpy(m_pbuf, rec.m_pbuf, m_size);
            }

            return *this;
        }

        uint8_t get_type() const 
        { 
            if (get_keysize() > 0)
            {
                if (get_valuesize() > 0)
                    return RECORD_SMALL;
                return RECORD_MEDIUM;
            }

            if (get_overflow_next() > 0)
                return RECORD_LARGE;

            return RECORD_EMPTY;
        } 

        void set_hash32(uint32_t hash) { set_uint32(m_pbuf, RECORD_HASH32, hash); }
        uint32_t get_hash32() const { return get_uint32(m_pbuf, RECORD_HASH32); } 

        void set_overflow_next(uint32_t ptr) { set_uint32(m_pbuf, RECORD_OVERFLOW_NEXT, ptr); }
        uint32_t get_overflow_next() const { return get_uint32(m_pbuf, RECORD_OVERFLOW_NEXT); } 

        void set_keysize(uint16_t size) { set_uint16(m_pbuf, RECORD_KEY_SIZE, size); }
        uint16_t get_keysize() const { return get_uint16(m_pbuf, RECORD_KEY_SIZE); }

        void set_valuesize(uint16_t size) { set_uint16(m_pbuf, RECORD_VALUE_SIZE, size); }
        uint16_t get_valuesize() const { return get_uint16(m_pbuf, RECORD_VALUE_SIZE); }  
       
        uint16_t get_payloadsize() const { return m_size - RECORD_END; }
        uint8_t *get_payload() { return m_pbuf + RECORD_END; }

        void *get_key() 
        {
            if (get_keysize() > 0)
                return get_payload();

            return NULL;
        }

        void *get_value() 
        {
            uint16_t len = get_keysize();
            if (len > 0 && get_valuesize() > 0)
                return get_payload() + len;

            return NULL;
        }
       
        //return the best compatible allocation strategy SMALL, MED, LARGE
        int fit_me(uint32_t key_size, uint32_t value_size) const;
        void set_payload(const void *key, uint16_t key_size); 
        void set_payload(const void *key, uint16_t key_size, 
                const void *value, uint16_t value_size);

    private:
        uint8_t *m_pbuf;
        uint16_t m_size;
    };


    class BucketBuf: public PageBuf
    {
    public:
        static const uint8_t BUCKET_MARKER = 'T';
        enum
        {
            BUCKET_NEXT_CHUNK = BASIC_END,
            BUCKET_END = BUCKET_NEXT_CHUNK + 4
        };

    public:
        BucketBuf(Pager &pgr, uint16_t rcount)
            :PageBuf(pgr, BUCKET_END) 
        {
            m_recsize = get_payloadsize() / rcount;  
            m_iter_index = 0;
            assert(m_recsize > Record::RECORD_END);
        }
        ~BucketBuf() {}
        //validate..
        bool validate() { return get_type() == BUCKET_MARKER; }
        void create()
        {
            set_type(BUCKET_MARKER);
            set_size(0);
            set_next(0);
            set_bucket_next(0);
        }
        
        void set_bucket_next(uint32_t ptr) { set_uint32(m_buf, BUCKET_NEXT_CHUNK, ptr); }
        uint32_t get_bucket_next() const { return get_uint32(m_buf, BUCKET_NEXT_CHUNK); }
        uint16_t max_records() const { return get_payloadsize() / m_recsize; }
        uint16_t record_size() const { return m_recsize; }
        bool empty() const { return !get_size(); }
        bool full() const { return get_size() == max_records(); } 
        
        bool get_record(Record &rec, uint16_t index);
        bool open_record(uint16_t index) { Record rec; return !get_record(rec, index); }
        uint16_t count_records();
        void remove_record(Record &rec);
        bool insert_record(Record &rec);

        void iter_rewind() { m_iter_index = 0; }
        bool iter_next();
        bool free_next();
        Record &record() { return m_iter_rec; }

    private:
        uint16_t m_recsize;

        //iterate over records in this bucket
        Record m_iter_rec;
        uint16_t m_iter_index;
    };
    

}

#endif
