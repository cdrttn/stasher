#ifndef _PAGELIST_H_
#define _PAGELIST_H_

#include "pager.h"
#include <assert.h>
#include <iterator>

namespace ST
{
    template <class PageT, class Pointer, class Reference>
    struct PageIter: std::iterator<std::bidirectional_iterator_tag, PageT, ptrdiff_t, Pointer, Reference>
    {
        typedef PageIter<PageT, PageT *, PageT &> iterator;
        typedef PageIter<PageT, const PageT *, const PageT &> const_iterator;

        PageIter () {}
        //can convert iterator to const_iterator
        PageIter(const iterator &pi) {m_nil = pi.m_nil; m_page = pi.m_page;}
        Reference operator*() const { return m_page; }
        Pointer operator->() const { return &m_page; }

        bool operator==(const iterator &r) { return m_page == r.m_page; }
        bool operator!=(const iterator &r) { return !(*this == r); }
        bool operator==(const const_iterator &r) { return m_page == r.m_page; }
        bool operator!=(const const_iterator &r) { return !(*this == r); }
 
        PageIter &operator++()
        {
            read(m_page.get_next());
            return *this;
        }   

        PageIter operator++(int)
        {
            PageIter tmp(*this);
            operator++();
            return tmp;
        }
 
        PageIter &operator--()
        {
            read(m_page.get_prev()); 
            return *this;
        }   

        PageIter operator--(int)
        {
            PageIter tmp(*this);
            operator--();
            return tmp;
        } 

        PageIter(const PageT &nil, uint32_t page)
        {
            m_nil = nil;
            read(page);
        }

        void read(uint32_t pg)
        {
            Pager *pgr = m_nil.pager();
            assert(pgr);
            if (!pgr->deref_page(m_page, pg))
                m_page = m_nil;
        }

        mutable PageT m_page;
        mutable PageT m_nil;
    };
   
    template <class PageT>
    class PageList
    {
    public:
        typedef PageIter<PageT, PageT *, PageT &> iterator;
        typedef PageIter<PageT, const PageT *, const PageT &> const_iterator;

        PageList(Pager &pgr, uint32_t hptr = 0)
            :m_pager(pgr) 
        {
            m_pager.new_page_tmp(m_nil);
            m_nil.set_next(hptr);
            if (hptr)
            {
                PageT pg;
                m_pager.read_page(pg, hptr);
                m_nil.set_prev(pg.get_tail());        
            }
        }

        virtual ~PageList()
        {
            try
            { sync(); }
            catch (...) {}
        }

        void sync()
        {
            PageT head;
            
            if (!m_pager.writeable())
                return;
            if (m_pager.deref_page_dirty(head, get_head()))
                head.set_tail(get_tail()); 
        }

        uint32_t get_head() const { return m_nil.get_next(); }
        uint32_t get_tail() const { return m_nil.get_prev(); }

        iterator begin() { return iterator(m_nil, get_head()); }
        iterator end() { return iterator(m_nil, 0); }
        const_iterator begin() const { return const_iterator(m_nil, get_head()); }
        const_iterator end() const { return const_iterator(m_nil, 0); }

        //XXX take a reference to iterator to return the page/invalidate iter
        void erase(iterator &iter)
        {
            PageT l, r;
            
            assert(iter.m_nil == m_nil && iter.m_page != m_nil);

            read(l, iter->get_prev());
            read(r, iter->get_next()); 
            l.set_next(r.get_page());
            r.set_prev(l.get_page()); 
            l.make_dirty();
            r.make_dirty();
            m_pager.free_page(*iter);
        } 

        //insert before iter
        void insert(iterator iter, PageT &page)
        {
            PageT l;

            m_pager.new_page(page);
            read(l, iter->get_prev());
            page.set_prev(l.get_page());
            page.set_next(iter->get_page());
            iter->set_prev(page.get_page());            
            l.set_next(page.get_page());
            
            iter->make_dirty();
            l.make_dirty();
        }

        void push_front(PageT &page)
        {
            insert(begin(), page);    
        }

        void push_back(PageT &page)
        {
            insert(end(), page);    
        }

        void clear()
        {
            iterator it;
            while ((it = begin()) != end())
                erase(it);    
        }

        uint32_t size() const
        {
            uint32_t i;
            const_iterator it;

            for (i = 0, it = begin(); it != end(); ++it, ++i)
                ;
            
            return i; 
        }

        bool empty() const { return begin() == end(); }

    private:
        Pager &m_pager;
        PageT m_nil;

        void read(PageT &pg, uint32_t pgno)
        {
            if (!m_pager.deref_page(pg, pgno))
                pg = m_nil;
        }
    }; 
}

#endif
