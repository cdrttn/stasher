#ifndef _STASHER_H_
#define _STASHER_H_

#include "ints.h"
#include "exceptions.h"
#include "data.h"
#include <string>

using namespace std;

namespace ST
{
    typedef uint32_t (*hashfunc_t)(const void *, uint32_t);
    
    class StasherOpts;
    class Stasher;
    class Pager;
    class HashHeaderBuf;
    class BucketArray;
    class BucketIter;

    class StasherOpts
    {
    public:
        static const uint16_t MAXEXP_DEFAULT = 12; //4096

    public:
        friend class Stasher;

        StasherOpts(hashfunc_t hash = NULL, int mode = 0644, uint16_t psz = 0)
        {
            m_hashfunc = hash;
            m_mode = mode;
            m_pagesize = psz;
            
            m_dupe = false;
            m_maxexp = MAXEXP_DEFAULT;
            m_maxrec = 16;
            m_capacity = 0; //use maxrec
        }

        //use NULL for default function
        void set_hash_func(hashfunc_t hash) { m_hashfunc = hash; }
        hashfunc_t get_hash_func() const { return m_hashfunc; }

        //access mode for file creation
        void set_access_mode(int mode) { m_mode = mode; }
        int get_access_mode() const { return m_mode; }

        //if not set, pagesize is derived from getpagesize() or similar syscall,
        //or read from a previously created file
        void set_page_size(uint16_t psz) { m_pagesize = psz; }
        uint16_t get_page_size() const { return m_pagesize; }

        //allow duplicate keys in the file
        void set_dupe_keys(bool dupe) { m_dupe = dupe; }
        bool get_dupe_keys() const { return m_dupe; }
       
        //max_chunk refers to the maximum number of bucket pages that can be 
        //appended when growing the bucket array. It is rounded to the nearest 
        //power of two.
        void set_bucket_max_chunk(uint16_t max);
        uint16_t get_bucket_max_chunk() const { return 1 << m_maxexp; }

        //max_records is the maximum number of records that can be stored in
        //a single bucket. Given that a bucket is the size of a page, the
        //size of a single record is around page_size / max_records. Note
        //that this is only an estimate; the actual size will be less because of
        //page and record metadata.
        void set_bucket_max_records(uint16_t recs) { m_maxrec = recs; }
        uint16_t get_bucket_max_records() const { return m_maxrec; }

        //capacity tells Stasher when to increase or shrink the number of 
        //addressable buckets by performing the linear hash split or join 
        //operations. The default is the same as max_records; however it can
        //be set higher or lower for space/time tradeoff.
        void set_bucket_capacity(uint16_t capacity) { m_capacity = capacity; }
        uint16_t get_bucket_capacity() const { return m_capacity; }

    private:
        bool m_dupe;
        int m_mode;
        uint16_t m_pagesize; 
        hashfunc_t m_hashfunc;        

        //bucket config
        uint8_t m_maxexp;
        uint16_t m_maxrec;
        uint16_t m_capacity;
    };

    class Stasher
    {
    public:
        enum
        {
            OPEN_READ = 1<<1,
            OPEN_WRITE = 1<<2,
        };

    public:
        Stasher();
        ~Stasher();

        void open(const string &filename, int flags = OPEN_READ, const StasherOpts &opts = StasherOpts());
        void close();

        uint64_t length();
        
        //returns false if there is a duplicate entry if no dupes is set
        bool put(const void *key, uint32_t klen, const void *value, uint32_t vlen);
        bool put(const buffer &key, const buffer &value) 
        { 
            return put(&key[0], key.size(), &value[0], value.size()); 
        }
        bool put(const string &key, const string &value) 
        { 
            return put(key.data(), key.size(), value.data(), value.size()); 
        }
      
        
        //returns true and first value found for key, or false if not found
        bool get(const void *key, uint32_t klen, buffer &value);
        bool get(const buffer &key, buffer &value)
        {
            return get(&key[0], key.size(), value);
        }
        //convenience function makes a copy to a string
        bool get(const string &key, string &value)
        {
            buffer tmp;
            bool ret = get(key.data(), key.size(), tmp);

            if (ret)
                value.assign((char *)&tmp[0], tmp.size());
            
            return ret;
        }

        //removes all entries with key, returning true, or false if no matches
        bool remove(const void *key, uint32_t klen);
        bool remove(const buffer &key)
        {
            return remove(&key[0], key.size());
        }
        
    private:
        void init();
        uint32_t address(uint32_t hash32);
        void split_bucket();
        void join_bucket();
        uint32_t do_hash(BucketIter &iter);

    private:
        bool m_opened;
        string m_filename;
        hashfunc_t m_hashfunc;

        Pager *m_pager;
        HashHeaderBuf *m_hhb;
        BucketArray *m_array;
    };
}

#endif
