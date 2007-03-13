#ifndef _PAGELIST_H_
#define _PAGELIST_H_

#include "pager.h"
#include <assert.h>
#include <iterator>

namespace ST
{
    template <class PageT>
    class PageIter: std::iterator<std::bidirectional_iterator_tag, PageT>
    {
    public:
        PageT &operator*() const { return *((PageT*)&m_page); }
        PageT *operator->() const { return (PageT*)&m_page; }
        bool operator==(const PageIter &r) { return m_page == r.m_page; }
        bool operator!=(const PageIter &r) { return !(*this == r); }
        
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

    private:
        PageT m_page;
        PageT m_nil;

        void read(uint32_t pg)
        {
            Pager *pgr = m_nil.pager();
            assert(pgr);
            if (!pgr->deref_page(m_page, pg))
                m_page = m_nil;
        }
     
        template <class> friend class PageList;
    };
   
    template <class PageT>
    class PageList
    {
    public:
        typedef PageIter<PageT> iterator;

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
            if (m_pager.deref_page_dirty(head, m_nil.get_next()))
                head.set_tail(m_nil.get_prev()); 
        }

        uint32_t get_head() const { return m_nil.get_next(); }
        uint32_t get_tail() const { return m_nil.get_prev(); }

        iterator begin()
        {
            iterator it;
            it.m_nil = m_nil;
            it.read(m_nil.get_next());
            return it;
        }
        
        iterator end()
        {
            iterator it;
            it.m_nil = m_nil;
            it.m_page = m_nil;
            return it; 
        }

        void erase(iterator iter)
        {
            PageT l, r;
            
            assert(*iter != m_nil);

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

    private:
        Pager &m_pager;
        uint32_t m_head;
        PageT m_nil;

        void read(PageT &pg, uint32_t pgno)
        {
            if (!m_pager.deref_page(pg, pgno))
                pg = m_nil;
        }
    }; 
}

#endif
