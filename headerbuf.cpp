#include "headerbuf.h"
#include "data.h"
#include <assert.h>
#include <string>

using namespace std;
using namespace ST;

bool HeaderBuf::validate()
{
    if (get_type() == MAGIC_TYPE && 
            get_magic() == MAGIC_CHECK && 
            get_pagesize() == get_file_pagesize() &&
            get_size() == count_freenodes())
        return true;

    return false;
}

uint16_t HeaderBuf::check_header(const buffer &head)
{
    const uint8_t *buf = &head[0];
    uint8_t type;
    uint8_t magic[MAGIC_LEN];
    uint16_t fileps;

    if (head.size() != HEADER_END)
        return 0;
    
    type = get_uint8(buf, PageBuf::HEADER_TYPE);
    fileps = get_uint16(buf, HeaderBuf::HEADER_PAGESZ);
    memcpy(magic, buf + HeaderBuf::HEADER_MAGIC, MAGIC_LEN);

    if (type == MAGIC_TYPE && !memcmp(magic, MAGIC_CHECK, MAGIC_LEN))
        return fileps;

    return 0;
}

void HeaderBuf::create()
{
    set_magic(MAGIC_CHECK);
    set_type(MAGIC_TYPE);
    set_size(0);
    set_next(0);

    //set the pagesize to be written to the file
    set_file_pagesize(get_pagesize());
}

//XXX: for get_freenode and scan_freenode the headerbuf (this) 
//     needs to persist as long as the FreeNode persists 
FreeNode *HeaderBuf::get_freenode(uint16_t index)
{
    assert(index < max_freenodes());
    
    uint8_t *ptr = get_payload() + index * 8;
    return new FreeNode(ptr, this);            
}

//same as above, but return NULL if no node is present at index
FreeNode *HeaderBuf::scan_freenode(uint16_t index)
{
    assert(index < max_freenodes());

    uint8_t *ptr = get_payload() + index * 8;
    uint64_t *check = (uint64_t *)ptr;

    if (*check != 0)
        return new FreeNode(ptr, this);

    return NULL;
}

string HeaderBuf::get_magic() const
{
    char buf[MAGIC_LEN];
    memcpy(buf, m_buf + HEADER_MAGIC, MAGIC_LEN);
    string ret(buf, MAGIC_LEN);

    return ret;
}

FreeNode *HeaderBuf::get_next_freenode()
{
    int index = max_freenodes() - 1;
    uint64_t *p = (uint64_t *) get_payload();

    while (index >= 0)
    {
        if (p[index] == 0)
        {
            FreeNode *node = get_freenode(index); 
            node->set_offset(1);
            node->set_span(1);
            incr();
            return node;
        }

        index--;
    }
   
    return NULL;
}

uint16_t HeaderBuf::count_freenodes() 
{
    int index = max_freenodes() - 1;
    uint64_t *p = (uint64_t *) get_payload();
    uint16_t count = 0;

    while (index >= 0)
    {
        if (p[index] != 0)
            count++;
        
        index--;
    }
   
    return count;
}


