#include "hashfunc.h"
#include <stdio.h>
#include <string.h>

// ripped from hash_func.c of the berkeley db
// COPYRIGHT ???

#define DCHARHASH(h, c) ((h) = 0x63c63cd9*(h) + 0x9c39c33d + (c))
uint32_t hash_func1(const void *key, uint32_t len)
{
        const uint8_t *e, *k;
        uint32_t h;
        uint8_t c;

        k = (const uint8_t *)key;
        e = k + len;
        for (h = 0; k != e;) {
                c = *k++;
                if (!c && k > e)
                        break;
                DCHARHASH(h, c);
        }
        return (h);
}

uint32_t hash_func2(const void *key, uint32_t len)
{
        const uint8_t *k, *e;
        uint32_t h;

        k = (const uint8_t *)key;
        e = k + len;
        for (h = 0; k < e; ++k) {
                h *= 16777619;
                h ^= *k;
        }
        return (h);
}

uint32_t hash_func3(const void *key, uint32_t len)
{
        const uint8_t *k;
        uint32_t h, loop;

        if (len == 0)
                return (0);

#define HASH4a  h = (h << 5) - h + *k++;
#define HASH4b  h = (h << 5) + h + *k++;
#define HASH4   HASH4b
        h = 0;
        k = (const uint8_t *)key;

        loop = (len + 8 - 1) >> 3;
        switch (len & (8 - 1)) {
        case 0:
                do {
                        HASH4;
        case 7:
                        HASH4;
        case 6:
                        HASH4;
        case 5:
                        HASH4;
        case 4:
                        HASH4;
        case 3:
                        HASH4;
        case 2:
                        HASH4;
        case 1:
                        HASH4;
                } while (--loop);
        }
        return (h);
}

uint32_t hash_func4(const void *key, uint32_t len)
{
        const uint8_t *k;
        uint32_t n, loop;

        if (len == 0)
                return (0);

#define HASHC   n = *k++ + 65599 * n
        n = 0;
        k = (const uint8_t *)key;

        loop = (len + 8 - 1) >> 3;
        switch (len & (8 - 1)) {
        case 0:
                do {
                        HASHC;
        case 7:
                        HASHC;
        case 6:
                        HASHC;
        case 5:
                        HASHC;
        case 4:
                        HASHC;
        case 3:
                        HASHC;
        case 2:
                        HASHC;
        case 1:
                        HASHC;
                } while (--loop);
        }
        return (n);
}

#ifdef HASH_TEST
// silly test
int main()
{
    char buf[256];
    while (gets(buf))
    {
        uint32_t h1, h2, h3, h4;
        uint32_t len = strlen(buf);

        h1 = hash_func1(buf, len);
        h2 = hash_func2(buf, len);
        h3 = hash_func3(buf, len);
        h4 = hash_func4(buf, len);

        printf("hash_func1(%s) -> %u\n", buf, h1);
        printf("hash_func2(%s) -> %u\n", buf, h2);
        printf("hash_func3(%s) -> %u\n", buf, h3);
        printf("hash_func4(%s) -> %u\n", buf, h4);
    }

    return 0;
}
#endif
