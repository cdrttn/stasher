#ifndef _BUCKET_H_
#define _BUCKET_H_

#include "data.h"

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
        uint32_t get_hash32();

        bool next();

    private:
        BucketIter(BucketBuf *iter);
        void load_overflow();
        void copy(const BucketIter &iter);
        
        uint32_t m_pptr;
        BucketBuf *m_iter; 
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

        void append(uint32_t hash32, const buffer &key, const buffer &value)
        {
            append(hash32, &key[0], key.size(), &value[0], value.size());
        }

        void append(uint32_t hash32, const void *key, uint32_t ksize, 
                const void *value, uint32_t vsize);
        void copy_quick(BucketIter &ito, BucketIter &ifrom);
        void remove(BucketIter &iter, int cleanup = 0);
        void compact();
        void clear(); 
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
