#ifndef _BUCKETBUF_H_
#define _BUCKETBUF_H_

#include "pager.h"
#include "record.h"
#include <assert.h>

namespace ST
{
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
        bool empty() const { return get_size() == get_payloadsize(); }
        bool full() const { return !get_size(); } 
        uint16_t get_usedsize() const { return get_payloadsize() - get_size(); }
        uint16_t get_freesize() const { return get_size(); }
        void set_freesize(uint16_t fsz) { set_size(fsz); }
        uint16_t count_records();

        bool insert_record(Record &rec);
        void remove_record(Record &rec);

        void first_record(Record &rec);
        bool next_record(Record &rec);
    };
    

}

#endif
