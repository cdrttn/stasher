#ifndef _INTS_H_
#define _INTS_H_

#include <sys/types.h>
#include <string.h>

#ifdef HAVE_STDINT
#include <stdint.h>
#elif VISUALC

typedef __int8 int8_t;
typedef unsigned __int8 uint8_t;
typedef __int16 int16_t;
typedef unsigned __int16 uint16_t;
typedef __int32 int32_t;
typedef unsigned __int32 uint32_t;
typedef __int64 int64_t;
typedef unsigned __int64 uint64_t;

#else
#error "Need integer types."
#endif

//FIXME: these should care about endian-ness
inline void set_uint8(uint8_t *buf, size_t poffset, uint8_t value)
{
    buf[poffset] = value;
}

inline uint8_t get_uint8(const uint8_t *buf, size_t poffset) 
{
    return buf[poffset];
}

inline void set_uint16(uint8_t *buf, size_t poffset, uint16_t value)
{
    memcpy(buf + poffset, &value, sizeof(value)); 
}

inline uint16_t get_uint16(const uint8_t *buf, size_t poffset) 
{
    uint16_t value;
    memcpy(&value, buf + poffset, sizeof(value)); 
    return value;
}

inline void set_uint32(uint8_t *buf, size_t poffset, uint32_t value)
{
    memcpy(buf + poffset, &value, sizeof(value)); 
}

inline uint32_t get_uint32(const uint8_t *buf, size_t poffset)
{
    uint32_t value;
    memcpy(&value, buf + poffset, sizeof(value)); 
    return value;
}

inline void set_uint64(uint8_t *buf, size_t poffset, uint64_t value)
{
    memcpy(buf + poffset, &value, sizeof(value)); 
}

inline uint64_t get_uint64(const uint8_t *buf, size_t poffset)
{
    uint64_t value;
    memcpy(&value, buf + poffset, sizeof(value)); 
    return value;
}

inline int log2(uint32_t num)
{
    int power = 31;

    while (num && !(num & 0x80000000))
    {
        num <<= 1;
        power--;
    }

    return power;
}

inline uint32_t pow2(int exp)
{
    return 1 << exp;
}
#endif
