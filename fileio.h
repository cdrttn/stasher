//Low level file IO wrapper with exceptions.

#ifndef _FILEIO_H_
#define _FILEIO_H_ 

#include <fcntl.h>
#include <string>
#include "ints.h"
#include "exceptions.h"

//needs typedef
#include <unistd.h>


using namespace std;

namespace ST
{
    //XXX: should fileio close file in destructor?
    class FileIO
    {
    public:
        FileIO(int fd = -1): m_fd(fd) {}
        
        void open(const string &path, int flags, int mode = 0644);
        void close();
        size_t read(void *buf, size_t nbytes); 
        size_t write(const void *buf, size_t nbytes);
        int64_t seek(int64_t offset, int whence = SEEK_SET);
        void truncate(int64_t length);
        int64_t size();
        static bool exists(const string &path);
        static void unlink(const string &path);
        static uint16_t getpagesize();

        int get_fd() { return m_fd; }

    private:
        int m_fd;

    private:
        FileIO(const FileIO &);
        FileIO &operator=(const FileIO &);
    }; 

}

#endif
