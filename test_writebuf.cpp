#include "pager.h"
#include "data.h"
#include <stdio.h>
#include <string>
#include <sys/mman.h>

using namespace ST;
using namespace std;

void copyinto(Pager &pgr, const char *fn)
{
    FileIO io;
    uint8_t *buf;
    
    io.open(fn, O_RDONLY);
    uint64_t size = io.size();
 
    buf = (uint8_t *)mmap(NULL, size, PROT_READ, MAP_PRIVATE, io.get_fd(), 0);
    assert(buf != NULL);

    printf("pages writen -> %d\n", pgr.write_buf(buf, size, 1));

    assert(munmap(buf, size) != -1);    

    io.close();

    buffer buf2(size);
    pgr.read_buf(&buf2[0], size, 1);
    write(2, &buf2[0], size);  
}

int main(int argc, char **argv)
{
    if (argc < 2)
    {
        puts("writebuf db file1 file2 ...");
        return 0;
    }

    Pager pgr;

    pgr.open(argv[1], true);
    printf("opened -> %s, pages -> %d\n", pgr.filename().c_str(), pgr.length());

    for (int i = 2; i < argc; i++)
        copyinto(pgr, argv[i]);
 
    return 0;
}

