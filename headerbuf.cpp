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
    fileps = get_uint16(buf, HEADER_PAGESZ);
    memcpy(magic, buf + HEADER_MAGIC, MAGIC_LEN);

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
    set_file_pagecount(1);
}

bool HeaderBuf::add_freenode(uint32_t offset, uint32_t span)
{
    uint8_t *ptr;
   
    if (get_size() == max_freenodes())
        return false;
    
    ptr = get_payload() + get_size() * 8;

    set_uint32(ptr, 0, offset);
    set_uint32(ptr, 4, span);
    incr();

    return true;
}

void HeaderBuf::get_freenode(uint16_t index, uint32_t &offset, uint32_t &span)
{
    assert(index < get_size());
    
    uint8_t *ptr = get_payload() + index * 8;
    offset = get_uint32(ptr, 0);
    span = get_uint32(ptr, 4);
}

string HeaderBuf::get_magic() const
{
    char buf[MAGIC_LEN];
    memcpy(buf, get_buf() + HEADER_MAGIC, MAGIC_LEN);
    string ret(buf, MAGIC_LEN);

    return ret;
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


