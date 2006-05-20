#ifndef _HEADERBUF_H_
#define _HEADERBUF_H_

#include "pager.h"
#include "data.h"

#define MAGIC_TYPE  'H'
#define MAGIC_CHECK "PAGER"
#define FREE_CHUNK 8

namespace ST
{
    class HeaderBuf;
    class FreeNode;

    //identify the header page of a Pager file.
    class HeaderBuf: public PageBuf
    {
    public:
        friend class FreeNode;
        enum
        {
            MAGIC_LEN = 5,
            HEADER_MAGIC = BASIC_END,
            HEADER_PAGESZ = BASIC_END + MAGIC_LEN, 
            HEADER_END = HEADER_PAGESZ + 2 
        };
    public:
        HeaderBuf(Pager &pgr): PageBuf(pgr, HEADER_END) {}
        virtual ~HeaderBuf() {}

        virtual bool validate();
        void create();
        FreeNode *get_freenode(uint16_t index);
        FreeNode *scan_freenode(uint16_t index);
        FreeNode *get_next_freenode(); //get next open freenode
        uint16_t count_freenodes();
        uint16_t max_freenodes() { return get_payloadsize() / FREE_CHUNK; }
        void set_magic(const char *magic) { memcpy(m_buf + HEADER_MAGIC, magic, MAGIC_LEN); } 
        string get_magic() const;

        void set_file_pagesize(uint16_t psz) { set_uint16(m_buf, HEADER_PAGESZ, psz); }
        uint16_t get_file_pagesize() const { return get_uint16(m_buf, HEADER_PAGESZ); }

        //peak at a header. return page size or 0 for error
        static uint16_t check_header(const buffer &head);
        
    protected:
        HeaderBuf(Pager &pgr, uint16_t metasize): PageBuf(pgr, metasize) {}

    private:
        void decr() { set_size(get_size()-1); }
        void incr() { set_size(get_size()+1); }
    };

    //class operates on 8 byte chunks of the header table's payload
    class FreeNode
    {
    public:
        friend class HeaderBuf;
        enum
        {
            FREE_OFFSET = 0,
            FREE_SPAN = 4
        };
        
    public:
        void set_offset(uint32_t offset) { set_uint32(m_pbuf, FREE_OFFSET, offset); }
        uint32_t get_offset() const { return get_uint32(m_pbuf, FREE_OFFSET); }
        void set_span(uint32_t span) { set_uint32(m_pbuf, FREE_SPAN, span); }  
        uint32_t get_span() const { return get_uint32(m_pbuf, FREE_SPAN); }  

        void remove() 
        { 
            memset(m_pbuf, 0, FREE_CHUNK); 
            m_header->decr();
            m_pbuf = NULL;
            delete this;
        }

        void update() { m_header->set_dirty(true); }

    private:
        FreeNode() {}
        FreeNode(uint8_t *buf, HeaderBuf *header): m_pbuf(buf), m_header(header) {}
        uint8_t *m_pbuf;
        HeaderBuf *m_header;
    };
}

#endif
