#ifndef _HEADERBUF_H_
#define _HEADERBUF_H_

#include "pager.h"
#include "data.h"
#include <memory>

using namespace std;
#define MAGIC_TYPE  'H'
#define MAGIC_CHECK "PAGER"
#define FREE_CHUNK 8

namespace ST
{
    class FreeNode
    {
    public:
        FreeNode(uint32_t offset = 0, uint32_t span = 0): 
            m_offset(offset), m_span(span) {}

        void set_offset(uint32_t offset) { m_offset = offset; }
        uint32_t get_offset() const { return m_offset; }
        void set_span(uint32_t span) { m_span = span; }
        uint32_t get_span() const { return m_span; }

    private:
        uint32_t m_offset;
        uint32_t m_span;
    };

    //identify the header page of a Pager file.
    class HeaderBuf: public PageBuf
    {
    public:
        enum
        {
            MAGIC_LEN = 5,
            HEADER_MAGIC = BASIC_END,
            HEADER_PAGESZ = HEADER_MAGIC + MAGIC_LEN, 
            HEADER_PAGECT = HEADER_PAGESZ + 2,
            HEADER_END = HEADER_PAGECT + 4 
        };
    public:
        HeaderBuf(Pager &pgr): PageBuf(pgr, HEADER_END) {}
        virtual ~HeaderBuf() {}

        virtual bool validate();
        void create();
        bool add_freenode(const FreeNode *node) { return add_freenode(node->get_offset(), node->get_span()); }
        bool add_freenode(uint32_t offset, uint32_t span);
        void get_freenode(uint16_t index, uint32_t &offset, uint32_t &span);
        uint16_t count_freenodes();
        uint16_t max_freenodes() { return get_payloadsize() / FREE_CHUNK; }

        void set_magic(const char *magic) { memcpy(m_buf + HEADER_MAGIC, magic, MAGIC_LEN); } 
        string get_magic() const;
        void set_file_pagesize(uint16_t psz) { set_uint16(m_buf, HEADER_PAGESZ, psz); }
        uint16_t get_file_pagesize() const { return get_uint16(m_buf, HEADER_PAGESZ); }
        void set_file_pagecount(uint32_t pc) { set_uint32(m_buf, HEADER_PAGECT, pc); }
        uint32_t get_file_pagecount() const { return get_uint32(m_buf, HEADER_PAGECT); }

        //peak at a header. return page size or 0 for error
        static uint16_t check_header(const buffer &head);
        
    protected:
        HeaderBuf(Pager &pgr, uint16_t metasize): PageBuf(pgr, metasize) {}

    private:
        void decr() { set_size(get_size()-1); }
        void incr() { set_size(get_size()+1); }
    };

}

#endif
