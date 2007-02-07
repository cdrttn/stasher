#ifndef _RECORD_H_
#define _RECORD_H_

namespace ST
{
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
            RECORD_DUP,
            RECORD_MEDIUM,
            RECORD_LARGE
        };

    public:
        Record():
            m_type(RECORD_EMPTY) 
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

        void set_key(const void *key) { m_key = (uint8_t *)key; }
        const void *get_key() { return (void *)m_key; }

        void set_value(const void *value) { m_value = (const uint8_t *)value; }
        const void *get_value() { return (const void *)m_value; }

        uint16_t get_size() const { return m_size; }

        void first_value();
        bool next_value();
        
    private:
        int m_type;
        uint16_t m_size;
        const uint8_t *m_key;
        const uint8_t *m_value;
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
}

#endif

