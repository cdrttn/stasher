#ifndef _BUCKET_H_
#define _BUCKET_H_

#include "data.h"
#include "record.h"

namespace ST
{
    class BucketBuf;
    class OverflowBuf;
    class Pager;
    class PageBuf;
    class BucketIter;
    class Bucket;

    class BucketIter
    {
    public:
        friend class Bucket;
        
        BucketIter();
        BucketIter(const BucketIter &iter);
        BucketIter &operator=(const BucketIter &iter);

        ~BucketIter();

        void get_key(buffer &key);
        void get_value(buffer &value);
        //uint32_t get_hash32();
        Record &record() { return m_riter; }
        
        bool next();

    private:
        BucketIter(BucketBuf *iter);
        void load_overflow();
        void copy(const BucketIter &iter);
        
        uint32_t m_pptr;
        BucketBuf *m_biter; 
        Record m_riter;
        OverflowBuf *m_xtra;
    };

    class Bucket
    {
    public:
        enum
        {
            NOCLEAN = 1<<1,
            KEEPOVERFLOW = 1<<2
        };
    public:
        Bucket();
        Bucket(BucketBuf *head);
        ~Bucket();

        void set_head(BucketBuf *head);
        BucketBuf *get_head();

        bool append(uint32_t hash32, const buffer &key, const buffer &value)
        {
            return append(hash32, &key[0], key.size(), &value[0], value.size());
        }

        bool append(uint32_t hash32, const void *key, uint32_t ksize, 
                const void *value, uint32_t vsize);
        void remove(BucketIter &iter, int cleanup = 0);
        void compact();
        void clear(); 
        void copy_quick(BucketIter &ito, BucketIter &ifrom);
        bool empty();

        BucketIter iter();
        
    private:
        BucketBuf *m_head;

    private:
        Bucket(const Bucket &);
        Bucket &operator=(const Bucket &);
    };

}
#endif
