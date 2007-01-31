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

    public:
        Record():
            m_type(RECORD_EMPTY), m_size(0), m_key(NULL),
            m_value(NULL), m_hash32(0),
            m_overflow_next(0), m_keysize(0),
            m_valuesize(0), m_recptr(NULL), m_recptr_save(NULL)
        {}

        int get_type() const { return m_type; }  
        int set_type(uint16_t pgsz);

        uint32_t get_hash32() const { return m_hash32; }
        void set_hash32(uint32_t hash) { m_hash32 = hash; }

        void set_overflow_next(uint32_t ptr) { m_overflow_next = ptr; }
        uint32_t get_overflow_next() const { return m_overflow_next; } 

        void set_keysize(uint32_t size) { m_keysize = size; }
        uint32_t get_keysize() const { return m_keysize; }

        void set_valuesize(uint32_t size) { m_valuesize = size; }
        uint32_t get_valuesize() const { return m_valuesize; }

        void set_key(uint8_t *key) { m_key = key; }
        uint8_t *get_key() { return m_key; }

        void set_value(uint8_t *value) { m_value = value; }
        uint8_t *get_value() { return m_value; }

        uint16_t get_size() const { return m_size; }

    private:
        int m_type;
        uint16_t m_size;
        uint8_t *m_key;
        uint8_t *m_value;
        uint32_t m_hash32;
        uint32_t m_overflow_next;
        uint32_t m_keysize;
        uint32_t m_valuesize;

        uint8_t *m_recptr;
        uint8_t *m_recptr_save;

    private:
        void copyto(uint8_t *buf);
        void copyfrom(uint8_t *buf);
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
        BucketBuf(Pager &pgr)
            :PageBuf(pgr, BUCKET_END) 
        { }
        ~BucketBuf() {}
        //validate..
        bool validate() { return get_type() == BUCKET_MARKER; }
        void create()
        {
            set_type(BUCKET_MARKER);
            set_size(get_payloadsize());
            set_next(0);
            set_bucket_next(0);
        }
        
        void set_bucket_next(uint32_t ptr) { set_uint32(m_buf, BUCKET_NEXT_CHUNK, ptr); }
        uint32_t get_bucket_next() const { return get_uint32(m_buf, BUCKET_NEXT_CHUNK); }
        
        //size is the remaining bytes in this bucket
        bool empty() const { return get_size(); }
        bool full() const { return !get_size(); } 
        uint16_t get_usedsize() const { return get_payloadsize() - get_size(); }
        uint16_t count_records();

        bool insert_record(Record &rec);
        void remove_record(Record &rec);

        void first_record(Record &rec);
        bool next_record(Record &rec);
    };
    

}

#endif
