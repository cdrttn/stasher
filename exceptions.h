#ifndef _EXCEPTIONS_H_
#define _EXCEPTIONS_H_

#include <stdio.h>
#include <errno.h>
#include <string>

using namespace std;

namespace ST
{
    //base class for Stasher exceptions
    class Exception
    {
    public:
        Exception(const string &what, const string &file = "")
            : m_string(what), m_filename(file) { }
        string err_string() { return m_string; }
        string err_filename() { return m_filename; }

    private:
        string m_string;    
        string m_filename;
    };

    //general io exception 
    class IOException: public Exception
    {
    public:
        IOException(const string &what, const string &file = ""): Exception(what, file) {}
    };
    
    //invalid page (page unreadable, mangled, etc)
    class InvalidPageException: public IOException
    {
    public:
        InvalidPageException(const string &what, const string &file = ""): IOException(what, file) {}
    };

    //class that intercepts C errno 
    class CException: public Exception
    {
    public:
        CException(const string &what = "", const string &file = "")
            : Exception(what, file), m_strerr(strerror(errno)), m_errno(errno) {}

        int cerr_errno() { return m_errno; }
        string cerr_string() { return m_strerr; }
     
    private:
        string m_strerr;    
        int m_errno;
    };

    //for IO functions, open, fopen, read, write, etc
    class CIOException: public CException
    {
    public:
        CIOException(const string &what = "", const string &file = ""): CException(what, file) {}
    };
}

#endif
