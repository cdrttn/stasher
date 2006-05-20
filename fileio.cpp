#include "fileio.h"
#include <sys/types.h>
#include <sys/stat.h>

using namespace std;
using namespace ST;

//check return of a C function for error
#define CHECK(ret, what)  if (ret < 0) throw CIOException(what)    

void FileIO::open(const string &path, int flags, int mode)
{
    //FIXME
    flags |= O_LARGEFILE;

    if (flags & O_CREAT)
        m_fd = ::open(path.c_str(), flags, mode); 
    else
        m_fd = ::open(path.c_str(), flags); 

    if (m_fd < 0)
        throw CIOException("open", path);
}

void FileIO::close()
{
    if (m_fd != -1)
    {
        CHECK(::close(m_fd), "close");
        m_fd = -1;
    }
}

size_t FileIO::read(void *buf, size_t nbytes) 
{
    size_t ret = ::read(m_fd, buf, nbytes);
    CHECK(ret, "read");
    
    return ret;
}

size_t FileIO::write(const void *buf, size_t nbytes)
{
    size_t ret = ::write(m_fd, buf, nbytes);
    CHECK(ret, "write");

    return ret;
}

int64_t FileIO::seek(int64_t offset, int whence) 
{
    //need conditional compliation for windows
    int64_t ret = ::lseek(m_fd, offset, whence);
    CHECK(ret, "write");
     
    return ret;
}

void FileIO::truncate(int64_t length)
{
    CHECK(::ftruncate(m_fd, length), "truncate");   
}

bool FileIO::exists(const string &path)
{
    struct stat st;    
    return ::stat(path.c_str(), &st) != -1;
}

void FileIO::unlink(const string &path)
{
    CHECK(::unlink(path.c_str()), "unlink");
}

int64_t FileIO::size()
{
    struct stat st;    

    CHECK(::fstat(m_fd, &st), "fstat");

    return st.st_size;
}

uint16_t FileIO::getpagesize()
{
    //return sysconf(_SC_PAGESIZE);
    return ::getpagesize();
}
