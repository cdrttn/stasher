#include <stdio.h>
#include <string>
#include <iterator>
#include <algorithm>
#include "pager.h"
#include "bucketbuf.h"
#include "bucketlist.h"

using namespace ST;
using namespace std;


enum
{
    TEST_RECORD_DATA = META_RECORD_END,
    TEST_RECORD_SIZE = TEST_RECORD_DATA + 2,
    TEST_RECORD_END = TEST_RECORD_SIZE + 2
};

class TestRec: public MetaRecord<TEST_RECORD_END>
{
public:
    friend class TestBucketBuf;

public:
    uint8_t *get_data() { return meta_deref(get_dataptr()); }
    const uint8_t *get_data() const { return meta_deref(get_dataptr()); }
    uint16_t get_datasize() const { return get_uint16(m_meta, TEST_RECORD_SIZE); }

protected:
    uint16_t get_dataptr() const { return get_uint16(m_meta, TEST_RECORD_DATA); }
    void set_dataptr(uint16_t ptr) { set_uint16(m_meta, TEST_RECORD_DATA, ptr); }
    void set_datasize(uint16_t size) { set_uint16(m_meta, TEST_RECORD_SIZE, size); }

private:
    void update(uint16_t start, int size) 
    {
        if (get_dataptr() < start)
            set_dataptr(get_dataptr() + size);
    }
};

struct DataIn
{
    DataIn(void *d, uint16_t s, uint8_t t = 0)
        :data(d), size(s), tok(t) {}

    void *data;
    uint16_t size;
    uint8_t tok; 
};

class TestBucketBuf: public BucketBuf<TestRec, const DataIn&>
{
public:
    TestBucketBuf(Pager *pgr = NULL): 
        BucketBuf<TestRec, const DataIn&>(pgr, BucketBuf<TestRec, const DataIn&>::BUCKET_END, 'B')
        {}

    bool has_room(const DataIn &di) { return get_freesize() >= di.size + TEST_RECORD_END; }
    void push_back(const DataIn &di) { add_data(di.data, di.size, di.tok); }
    void push_front(const DataIn &di) { add_data(di.data, di.size, di.tok); }

    void add_data(void *data, uint16_t size, uint8_t tok = 0)
    {
        uint8_t *ptr;
        TestRec newrec;

        ptr = alloc_heap(size);
        memcpy(ptr, data, size);

        alloc_meta(newrec);
        newrec.set_type(tok);
        newrec.set_datasize(size);
        newrec.set_dataptr(ptr_meta(ptr));
    }

    //remove has effect of item++
    void remove(iterator item)
    {
        printf("removing %d\n", item->get_type());    
        free_heap(meta_ptr(item->get_dataptr()), item->get_datasize());
        free_meta(*item);
    }
};

bool cmp(const TestRec &l, const TestRec &r) 
{ 
    return strcmp((const char *)l.get_data(), (const char*)r.get_data()) < 0;
}

void write_test(const char *file)
{
    char data[256];
    int i;
    Pager pgr;
    TestBucketBuf buf;

    pgr.open(file, Pager::OPEN_WRITE);
    printf("opened(write) -> %s, pages -> %d\n", pgr.filename().c_str(), pgr.length());
    BucketList<TestBucketBuf> blist(pgr);

    //pgr.new_page(buf);

    for (i = 0; i < 255; i++)
    {
        sprintf(data, "(%4d) testing one two three", i);
        blist.push_back(DataIn(data, strlen(data) + 1, i));
        /*
        DataIn di(data, strlen(data) + 1, i);

        if (buf.has_room(di))
            buf.push_back(di);
        else
            break;
        */
    }
    puts("HERE");
    //TestBucketBuf::iterator a, b, it = buf.begin();

    //a = buf.begin() + 5;
    //b = buf.begin() + 7;
    //printf("a(%d), b(%d)\n", a->get_type(), b->get_type());
    //iter_swap(a, b); 
    //printf("a(%d), b(%d)\n", a->get_type(), b->get_type());

    //random_shuffle(buf.begin(), buf.end());
    //sort(buf.begin(), buf.end(), cmp);
    //TestRec tmp;
    //while (buf.begin() != buf.end())
    //    buf.remove(buf.begin());
    //a = buf.begin() + 5; 
    //printf("will remove %d\n", buf.begin()[5].get_type());
    //tmp = *(buf.begin() + 5);
    //tmp = buf.begin()[5];
    //buf.remove(buf.begin() + 5); 
    //buf.remove(buf.end() - 5); 
    //printf("removed %d\n", tmp.get_type());
    //tmp = it[5];
    //it[5] = it[6];
    //it[6] = tmp;
    //*(it + 5) = *(it + 6);
    //iter_swap(it + 5, it + 6); 
    //a = ++it;
    //buf.remove(a); 
    //a = ++it;
    //buf.remove(a);
    //buf.remove(--buf.end());
    //buf.remove(--buf.end());
    //buf.remove(--buf.end());

    //for (it = buf.begin(); it < buf.end(); )
    //{
        //a = it++;
        //buf.remove(it);
    //}
    //buf.clear();
}

void read_test(const char *file)
{
    Pager pgr;
    TestBucketBuf buf;
    TestBucketBuf::iterator it;
    TestBucketBuf::const_iterator cit;

    pgr.open(file);
    printf("opened(read) -> %s, pages -> %d\n", pgr.filename().c_str(), pgr.length());

    pgr.read_page(buf, 1);
    puts("HERE2");
    for (cit = buf.begin(); cit < buf.end(); ++cit)
        printf("id(%4d) : %s\n", cit->get_type(), (char*)cit->get_data());
    sort(buf.begin(), buf.end(), cmp);
    cit = buf.begin();
    for (int i = 0; cit + i < buf.end(); ++i)
        printf("sort id(%4d) : %s\n", cit[i].get_type(), (char*)cit[i].get_data());
}

int main(int argc, char **argv)
{
    if (argc < 2)
    {
        puts("bbuf file");
        return 0;
    }

    write_test(argv[1]);
    puts("HERE1.5");
    read_test(argv[1]);

    return 0;
}

