#ifndef _BUCKETARRAY_H_
#define _BUCKETARRAY_H_

#include <vector>

using namespace std;

namespace ST
{
    class HashHeaderBuf;
    class Bucket;
    class Pager;

    class BucketArray
    {
    public:
        BucketArray(HashHeaderBuf &hhb);
        ~BucketArray();

        void get(Bucket &bucket, uint32_t index);
        void resize(uint32_t size);
        void append(Bucket &bucket);
        void last(Bucket &bucket);
        void shrink();
        uint32_t length();
       
    private:
        void read_heads();
        void find_chunk(uint32_t index, uint32_t &chunk, uint32_t &subindex);
        int chunk_length(int chunk);
        uint32_t chunk_offset(int chunk);
        void append_head(uint32_t offset); 
        void shrink_head();

    private:
        HashHeaderBuf &m_hhb;
        Pager &m_pager;
        uint8_t m_maxexp;
        vector<uint32_t> m_heads;
        vector<HashHeaderBuf *> m_headbufs;

    private:
        BucketArray &operator=(BucketArray &);
        BucketArray(BucketArray &);
    };
}

#endif
