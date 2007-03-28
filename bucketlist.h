#ifndef _BUCKETLIST_H_
#define _BUCKETLIST_H_

#include <assert.h>
#include "bucketbuf.h"
#include "pagelist.h"

namespace ST
{

    template <class RecT, class Pointer, class Reference, class BucketIterT, class RecIterT>
    struct BucketListIter: std::iterator<std::bidirectional_iterator_tag, RecT, ptrdiff_t, Pointer, Reference>
    {
        typedef BucketListIter<RecT, RecT *, RecT &> iterator;
        typedef BucketListIter<RecT, const RecT *, const RecT &> const_iterator;

        BucketListIter () {}
        //can convert iterator to const_iterator
        BucketListIter(const iterator &pi) {m_nil = pi.m_nil; m_bucket = pi.m_bucket;}
        Reference operator*() const { return *m_rec; }
        Pointer operator->() const { return &(*m_rec); }

        bool operator==(const iterator &r) { return m_rec == r.m_rec; }
        bool operator!=(const iterator &r) { return !(*this == r); }
        bool operator==(const const_iterator &r) { return m_rec == r.m_rec; }
        bool operator!=(const const_iterator &r) { return !(*this == r); }
 
        BucketListIter &operator++()
        {
            ++m_rec;

            if (m_rec == m_bucket->end() && m_bucket != m_bucket_end)
            {
                ++m_bucket;
                m_rec = m_bucket->begin();
            }                        

            //use the nil page
            if (m_rec == m_bucket->end())
                m_rec = m_bucket_end->end();

            return *this;
        }   

        BucketListIter operator++(int)
        {
            BucketListIter tmp(*this);
            operator++();
            return tmp;
        }
 
        BucketListIter &operator--()
        {
            return *this;
        }   

        BucketListIter operator--(int)
        {
            BucketListIter tmp(*this);
            operator--();
            return tmp;
        } 

        BucketListIter(BucketIterT page, BucketIterT end, RecIterT rec)
        {
            m_bucket = page;
            m_buck_end = end;
            m_rec = rec;
        }

        mutable BucketIterT m_bucket;
        mutable BucketIterT m_bucket_end;
        mutable RecIterT m_rec;
    };


    template <class BucketT>
    class BucketList
    {
    public:
        typedef typename BucketT::input_type input_type;
        typedef typename PageList<BucketT>::iterator page_iter;

        BucketList(Pager &pgr, uint32_t hptr = 0)
            :m_plist(pgr, hptr) { }
        
        virtual ~BucketList() 
        {
            try
            { sync(); }
            catch (...) {}
        }
        
        void sync() { m_plist.sync(); }
        uint32_t get_head() const { return m_plist.get_head(); }
        uint32_t get_tail() const { return m_plist.get_tail(); }

        void push_front(input_type data)
        {
            page_iter begin = m_plist.begin();
            BucketT page;

            if (begin == m_plist.end() || !begin->has_room(data))
                m_plist.push_front(page);
            else
                page = *begin;

            page.push_front(data);
            page.make_dirty(); 
        }

        void push_back(input_type data)
        {
            page_iter end = m_plist.end();
            BucketT page;

            if (end == m_plist.begin() || !(--end)->has_room(data))
                m_plist.push_back(page);
            else
                page = *end;

            page.push_back(data);
            page.make_dirty(); 
        }

        void push_best(input_type data)
        {
            page_iter it;
            BucketT page;

            it = m_plist.begin();
            while (it != m_plist.end() && !it->has_room(data))
                ++it;

            if (it == m_plist.end())
                m_plist.push_back(page);
            else
                page = *it;

            page.push_back(data);
            page.make_dirty();        
        }

    private:
        PageList<BucketT> m_plist;
    };
}

#endif
