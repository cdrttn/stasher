#ifndef _BUCKETBUF_H_
#define _BUCKETBUF_H_

#include "pager.h"
#include <assert.h>
#include <string.h>
#include <iterator>

namespace ST
{
    enum
    {
        META_RECORD_TYPE = 0,
        META_RECORD_END = META_RECORD_TYPE + 1
    };

    template <uint16_t MetaSize>
    class MetaRecord
    {
    public:
        template <class> friend class MetaIter;
        template <class> friend class BucketBuf;

        static const uint16_t metasize = MetaSize;

    public:
        MetaRecord(): m_meta(m_tmp), m_page(NULL) {} 
        MetaRecord(const MetaRecord &rec) { m_meta = m_tmp; m_page = NULL; copy(rec); }
        MetaRecord &operator=(const MetaRecord &rec) { copy(rec); return *this; }
        virtual ~MetaRecord() {}

        void set_type(uint8_t t) { *m_meta = t; }
        uint8_t get_type() const { return *m_meta; }

        void set_flag(uint8_t t) { *m_meta |= t; } 
        void unset_flag(uint8_t t) { *m_meta &= ~t; }
        bool has_flag(uint8_t t) const { return (*m_meta & t) != 0; }

    protected:
        uint8_t *meta_deref(uint16_t ptr)
        {
            assert(m_page);
            return m_page->get_payload() + ptr;
        } 

        const uint8_t *meta_deref(uint16_t ptr) const
        {
            assert(m_page);
            return m_page->get_payload() + ptr;
        } 

    protected:
        uint8_t *m_meta;
        PageBuf *m_page;
        
    private:
        //for any pointer < start, increment by size
        virtual void update(uint16_t start, int size) = 0;
        void set_meta(uint8_t *m) { m_meta = m; }
        void set_page(PageBuf *p) { m_page = p; }

        void copy(const MetaRecord &rec) 
        {
            printf("%p copy %p, %p meta %p\n", this, &rec, m_meta, rec.m_meta); 
            if (m_meta == rec.m_meta)
                return;
            assert(!m_page || m_page == rec.m_page);
            m_page = rec.m_page;
            memcpy(m_meta, rec.m_meta, metasize);
        } 

        uint8_t m_tmp[metasize];
    };


    template <class RecT>
    class MetaIter: public std::iterator<std::random_access_iterator_tag, RecT>
    {
    public:
        template <class> friend class BucketBuf;
        MetaIter() {}
        MetaIter(const MetaIter &iter) { copy(iter); }
        MetaIter &operator=(const MetaIter<RecT> &iter) { copy(iter); return *this; }
 
        RecT &operator*() const { return *((RecT*)&m_rec); }
        RecT *operator->() const { return (RecT*)&m_rec; }
        bool operator==(const MetaIter &r) const { return m_rec.m_meta == r.m_rec.m_meta; }
        bool operator!=(const MetaIter &r) const { return m_rec.m_meta != r.m_rec.m_meta; }
        bool operator<(const MetaIter &r) const { return m_rec.m_meta < r.m_rec.m_meta; }
        bool operator>(const MetaIter &r) const { return r < *this; } 
        bool operator<=(const MetaIter &r) const { return !(*this < r); }
        bool operator>=(const MetaIter &r) const { return !(r < *this); }

        MetaIter &operator++()
        {
            m_rec.m_meta += m_rec.metasize;
            return *this;
        }

        MetaIter operator++(int)
        {
            MetaIter tmp(*this);
            operator++();
            return tmp;     
        }
        
        MetaIter &operator--()
        {
            m_rec.m_meta -= m_rec.metasize;
            return *this;
        }

        MetaIter operator--(int)
        {
            MetaIter<RecT> tmp(*this);
            operator--();
            return tmp;     
        }

        MetaIter &operator+=(ptrdiff_t n)
        {   
            m_rec.m_meta += m_rec.metasize * n;
            return *this;            
        }

        MetaIter &operator-=(ptrdiff_t n) { return *this += -n; }

        const MetaIter operator+(ptrdiff_t n) const
        {
            MetaIter tmp(*this);
            return tmp += n;
        }

        const MetaIter operator-(ptrdiff_t n) const
        {
            MetaIter tmp(*this);
            return tmp -= n;
        }

        ptrdiff_t operator-(const MetaIter &r) const
        {
            return (m_rec.m_meta - r.m_rec.m_meta) / m_rec.metasize;
        }

        RecT operator[](ptrdiff_t n) const 
        { 
            RecT tmp;
            RecT &val = *(*this + n); 
            tmp.set_page(val.m_page);
            tmp.set_meta(val.m_meta);
            return tmp; 
        }

    private:
        MetaIter(PageBuf *page, uint8_t *start)
        {
            m_rec.set_page(page);
            m_rec.set_meta(start); 
        }
        
        //ensure a shallow copy
        void copy(const MetaIter &iter)
        {
            //printf("%p iter_shallow %p\n", this, &iter); 
            m_rec.m_page = iter.m_rec.m_page;
            m_rec.m_meta = iter.m_rec.m_meta;    
        }

    private:
        RecT m_rec;
    };
    
    template <class RecT>
    inline MetaIter<RecT> operator+(ptrdiff_t n, const MetaIter<RecT> &x) { return x + n; }

    template <class RecT>
    class BucketBuf: public PageBuf
    {
    public:
        enum
        {
            BUCKET_HEAPMETASIZE = BASIC_END,
            BUCKET_END = BUCKET_HEAPMETASIZE + 2
        };

        typedef MetaIter<RecT> iterator;

    public:
        virtual ~BucketBuf() {}
        
        bool validate() { return get_type() == m_bucketmarker; }
        virtual void create()
        {
            set_type(m_bucketmarker);
            set_freesize(get_payloadsize());
            set_heapmetasize(0);
            set_next(0);
        }

        iterator begin() { return iterator(this, get_payload()); }
        iterator end() { return iterator(this, get_metaend()); }
        uint16_t size() const { return (get_metaend() - get_payload()) / RecT::metasize; }

        //size is the remaining bytes in this bucket
        bool empty() const { return get_size() == get_payloadsize(); }
        bool full() const { return !get_size(); } 

    protected: 
        BucketBuf(Pager *pgr, uint32_t metaend, uint8_t marker)
            :PageBuf(pgr, metaend), m_bucketmarker(marker) 
        { }

        void set_heapmetasize(uint16_t sz) { set_uint16(get_buf(), BUCKET_HEAPMETASIZE, sz); }
        uint16_t get_heapmetasize() const { return get_uint16(get_buf(), BUCKET_HEAPMETASIZE); }

        void set_freesize(uint16_t fsz) { set_size(fsz); }
        uint16_t get_freesize() const { return get_size(); }
        uint16_t get_usedsize() const { return get_payloadsize() - get_size(); }

        uint8_t *get_metaend() { return get_payload() + get_heapmetasize(); }
        const uint8_t *get_metaend() const { return get_payload() + get_heapmetasize(); }
        uint8_t *get_heap() { return get_metaend() + get_freesize(); }

        uint16_t ptr_meta(uint8_t *ptr) { return ptr - get_payload(); }
        uint8_t *meta_ptr(uint16_t ptr) { return get_payload() + ptr; }

        uint8_t *alloc_heap(uint16_t size)
        {
            assert(size <= get_freesize());
            set_freesize(get_freesize() - size); 
            return get_heap();            
        }

        //this must update metadata
        void free_heap(uint8_t *start, uint16_t size)
        {
            assert(start >= get_heap() && start < get_end());
        
            memmove(get_heap() + size, get_heap(), start - get_heap());            
            memset(get_heap(), 0, size); //!! this is optional 
            set_freesize(get_freesize() + size);

            for (iterator i = begin(); i != end(); ++i)
            {
                MetaRecord<RecT::metasize> &rec = *i;
                rec.update(ptr_meta(start), size);
            }
        }

        void alloc_meta(RecT &rec)
        {
            uint16_t size = rec.metasize;

            assert(size <= get_freesize());
            rec.set_meta(get_metaend());
            rec.set_page(this);
            set_freesize(get_freesize() - size);
            set_heapmetasize(get_heapmetasize() + size);
        }

        void free_meta(RecT &rec)
        {
            uint16_t size = rec.metasize;
            uint8_t *dest = rec.m_meta;
            uint8_t *src = dest + size;

            assert(dest >= get_payload() && src <= get_metaend());
            memmove(dest, src, get_metaend() - src);

            set_freesize(get_freesize() + size);
            set_heapmetasize(get_heapmetasize() - size);
            memset(get_metaend(), 0, size); //!! optional
        }

    private:
        uint8_t m_bucketmarker;
    };
}

#endif
