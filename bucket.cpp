#include "bucket.h"
#include "bucketbuf.h"
#include "overflowbuf.h"
#include "exceptions.h"

using namespace std;
using namespace ST;

static void bucket_compact(BucketBuf *start);

Bucket::Bucket()
{
    m_head = NULL;
}

Bucket::Bucket(BucketBuf *head)
{
    m_head = head;
    //assert(head->get_page() > 0);
    //assert(head->allocated());
}


Bucket::~Bucket()
{
    delete m_head;
}

void Bucket::set_head(BucketBuf *head)
{
    delete m_head;
    m_head = head;
}

BucketBuf *Bucket::get_head()
{
    return m_head;
}

//Append a record to a bucket, adding overflows as neccesary. 
void Bucket::append(uint32_t hash32, const void *key, uint32_t ksize, 
    const void *value, uint32_t vsize)
{
    Pager &pgr = m_head->pager();
    BucketBuf *iter;
    BucketBuf tmp(pgr, m_head->max_records());

    if (!pgr.writeable())
        throw IOException("cannot append to readonly pagefile", pgr.filename());

    tmp.allocate();
    iter = m_head;

    while (1)
    {
        if (iter->get_size() < iter->max_records())
        {
            uint32_t ptr; 
            Record rec;
            if (!iter->insert_record(rec))
                throw InvalidPageException("Invalid overflow bucket (can't insert record)", pgr.filename());

            
            rec.set_hash32(hash32);
            switch (rec.fit_me(ksize, vsize))
            {
            case Record::RECORD_SMALL:
                rec.set_payload(key, ksize, value, vsize);
                rec.set_overflow_next(0);
                break;

            case Record::RECORD_MEDIUM:
                rec.set_payload(key, ksize);
                ptr = OverflowBuf::alloc_payload(pgr, value, vsize);
                rec.set_overflow_next(ptr);
                break;

            case Record::RECORD_LARGE:
                rec.set_keysize(0);
                rec.set_valuesize(0);
                ptr = OverflowBuf::alloc_payload(pgr, key, ksize, value, vsize);
                rec.set_overflow_next(ptr);
                break;
            }
           
            pgr.write_page(*iter);
            
            break;
        }
        
        if (iter->get_bucket_next())
        {
            pgr.read_page(tmp, iter->get_bucket_next());
            if (!tmp.validate())
                throw InvalidPageException("Invalid overflow bucket", pgr.filename());

            iter = &tmp;
        }
        else
        {
            uint32_t ptr = pgr.alloc_pages(1);
            iter->set_bucket_next(ptr);
            pgr.write_page(*iter);

            tmp.clear();
            tmp.create();
            pgr.write_page(tmp, ptr);

            iter = &tmp;
        }
    }
}

//directly copy a record from iter ifrom to the next available location
//in iter ito, preserving overflow ptrs. append overflow buckets as
//required. iter ito is advanced forward. ito and ifrom should not be 
//from the same bucket
void Bucket::copy_quick(BucketIter &ito, BucketIter &ifrom)
{
    BucketBuf *tobuf = ito.m_iter;
    BucketBuf *frombuf = ifrom.m_iter;
    Pager &pgr = tobuf->pager();

    if (!pgr.writeable())
        throw IOException("cannot append to readonly pagefile", pgr.filename());
    
    tobuf->iter_rewind();

    while (!tobuf->free_next())
    {
        if (tobuf->get_bucket_next())
            pgr.read_page(*tobuf, tobuf->get_bucket_next());
        else
        {
            uint32_t ptr = pgr.alloc_pages(1);
            tobuf->set_bucket_next(ptr);
            pgr.write_page(*tobuf);

            if (tobuf->get_page() == m_head->get_page())
                *m_head = *tobuf;
    
            tobuf->clear();
            tobuf->create();
            tobuf->set_page(ptr);
        }
        
        if (!tobuf->validate())
            throw InvalidPageException("Invalid overflow bucket", pgr.filename());

        tobuf->iter_rewind();
    }

    tobuf->record() = frombuf->record();
    tobuf->set_size(tobuf->get_size() + 1);
    pgr.write_page(*tobuf);

    if (tobuf->get_page() == m_head->get_page())
        *m_head = *tobuf;
    assert(tobuf->count_records() == tobuf->get_size());
}

//Move records from cur to prev. 
//prev is set to cur, cur is advanced, and the process continues 
//until there are no more buckets.
//XXX: error checking?
static void bucket_compact(BucketBuf *start)
{
    Pager &pgr = start->pager();
    uint16_t maxrec = start->max_records();
    BucketBuf tmp1(pgr, maxrec);
    BucketBuf tmp2(pgr, maxrec);
    BucketBuf *cur, *prev;

    prev = start;
    cur = &tmp1;
    while (prev->get_bucket_next())
    {
        prev->iter_rewind();
        cur->iter_rewind();

        pgr.read_page(*cur, prev->get_bucket_next()); 

        //move over available records from cur to prev
        while (cur->iter_next() && prev->free_next())
        {
            prev->record() = cur->record();
            prev->set_size(prev->get_size() + 1);
            cur->remove_record(cur->record());
        }

        //one or both of these conditions should be true..
        //there should never be any leftovers in cur if prev isn't full
        assert(cur->empty() || prev->full());

        //
        assert(cur->count_records() == cur->get_size() && 
                prev->count_records() == prev->get_size());

        //1. if current bucket empty, remove it from the file
        if (cur->empty())
        {
            prev->set_bucket_next(cur->get_bucket_next());
            pgr.free_pages(cur->get_page(), 1);
        }
        pgr.write_page(*prev);

        //2. if filled up prev bucket completely, move prev forward
        if (prev->full())
        {
            //in this case, cur was used up, and we must read ahead before the next pass.
            if (cur->empty())
            {
                if (cur->get_bucket_next())    
                    pgr.read_page(*cur, cur->get_bucket_next());
                else
                    break;
            }
            else
                pgr.write_page(*cur);

            //swap prev and cur, and preserve start
            BucketBuf *swap;
            if (prev == start)
                prev = &tmp2;

            swap = prev; 
            prev = cur;
            cur = swap;
        }
    }
}

#define MERGE_THRESHOLD 0.20

//Remove the current record in iter
//
//1. The head bucket must always have records in it if there are overflow buckets
//2. Buckets closer to the head should have more records
//
//If the load of a bucket drops below MERGE_THRESHOLD (method 1):
//      1. if there is a next bucket, fill with records from the next overflow buckets
//      2. if there is no next bucket (this is the tail bucket):
//          a. if records are still present, leave the bucket alone
//          b. if the bucket is not the head and there are no records present, remove the bucket
//
void Bucket::remove(BucketIter &iter, int cleanup) 
{
    BucketBuf *ibuck = iter.m_iter;
    Record &irec = ibuck->record();
    Pager &pgr = m_head->pager();
    BucketBuf tmp(pgr, m_head->max_records()); 

    if (!pgr.writeable())
        throw IOException("cannot remove from readonly pagefile", pgr.filename());

    ibuck->remove_record(irec);
    if (!(cleanup & KEEPOVERFLOW) && irec.get_overflow_next())
    {
        OverflowBuf obuf(pgr);
        pgr.read_page(obuf, irec.get_overflow_next());
        pgr.free_pages(obuf.get_page(), obuf.get_page_span());
    }

    //don't muckup the iterator unless told to
    if (!(cleanup & NOCLEAN))
    {
        float load = (float)ibuck->get_size() / (float)ibuck->max_records(); 

        if (load == 0.0 && iter.m_pptr)
        {
            BucketBuf *prev;

            if (iter.m_pptr == m_head->get_page())
                prev = m_head;
            else
            {
                prev = &tmp;
                pgr.read_page(*prev, iter.m_pptr);
            }

            prev->set_bucket_next(ibuck->get_bucket_next());
            pgr.free_pages(ibuck->get_page(), 1);
            pgr.write_page(*prev);
        }
        else if (load <= MERGE_THRESHOLD && ibuck->get_bucket_next())
        {
            bucket_compact(ibuck);
        }
    }

    pgr.write_page(*ibuck);
    
    //update changes to bucket head, if neccessary
    if (ibuck->get_page() == m_head->get_page())
        *m_head = *ibuck;
}

//Remove all bucket overflows, and record overflows.
//BucketArray owns the head bucket class Bucket is accessing, so it shouldn't be removed from the file.
void Bucket::clear() 
{
    BucketBuf iter(*m_head);
    Pager &pgr = m_head->pager();
    OverflowBuf obuf(pgr);

    if (!pgr.writeable())
        throw IOException("cannot remove from readonly pagefile", pgr.filename());

    m_head->clear_payload();
    m_head->set_size(0);
    m_head->set_bucket_next(0);
    pgr.write_page(*m_head);

    while (1)
    {
        int i;
        Record rec;

        for (i = 0; i < iter.max_records(); i++)
        {
            if (iter.get_record(rec, i) && rec.get_overflow_next())
            {
                pgr.read_page(obuf, rec.get_overflow_next());
                pgr.free_pages(obuf.get_page(), obuf.get_page_span());
            }            
        }
       
        //don't delete the head bucket
        if (iter.get_page() != m_head->get_page())
            pgr.free_pages(iter.get_page(), 1);

        if (iter.get_bucket_next())
            pgr.read_page(iter, iter.get_bucket_next());
        else
            break;
    }
}

void Bucket::compact()
{
    bucket_compact(m_head);
}

bool Bucket::empty()
{
    return !m_head->get_size();
}

BucketIter Bucket::iter()
{
    BucketIter iter(m_head);
    return iter;
}

BucketIter::BucketIter(BucketBuf *iter): 
    m_pptr(0), m_iter(iter), m_xtra(NULL)
{
    m_iter = new BucketBuf(*iter); 
    m_iter->iter_rewind();
}

BucketIter::BucketIter() 
{
    m_pptr = 0;
    m_iter = NULL; 
    m_xtra = NULL; 
}

BucketIter::BucketIter(const BucketIter &iter)
{
    copy(iter);
}

BucketIter &BucketIter::operator=(const BucketIter &iter)
{
    copy(iter);
    return *this;
}

void BucketIter::copy(const BucketIter &iter)  
{
    m_pptr = iter.m_pptr;
    if (iter.m_iter)
    {
        m_iter = new BucketBuf(*iter.m_iter);
        m_iter->iter_rewind();
    }
    else 
        m_iter = NULL;

    if (iter.m_xtra)
        m_xtra = new OverflowBuf(*iter.m_xtra);
    else
        m_xtra = NULL;
}

BucketIter::~BucketIter()
{
    delete m_iter;
    delete m_xtra;
}

//get data for current record
void BucketIter::get_key(buffer &key)
{
    Record &rec = m_iter->record();

    switch (rec.get_type())
    {
    case Record::RECORD_SMALL:
        key.resize(rec.get_keysize());
        memcpy(&key[0], rec.get_key(), rec.get_keysize());
        break;

    case Record::RECORD_MEDIUM:
        key.resize(rec.get_keysize());
        memcpy(&key[0], rec.get_key(), rec.get_keysize());
        break;

    case Record::RECORD_LARGE:
        load_overflow();
        m_xtra->get_key(key);
        break;
    }
}

void BucketIter::get_value(buffer &value)
{
    Record &rec = m_iter->record();

    switch (rec.get_type())
    {
    case Record::RECORD_SMALL:
        value.resize(rec.get_valuesize());
        memcpy(&value[0], rec.get_value(), rec.get_valuesize());
        break;

    case Record::RECORD_MEDIUM:
        load_overflow();
        m_xtra->get_value(value);
        break;

    case Record::RECORD_LARGE:
        load_overflow();
        m_xtra->get_value(value);
        break;
    }
}

void BucketIter::load_overflow()
{
    Pager &pgr = m_iter->pager();
    uint32_t optr = m_iter->record().get_overflow_next();

    if (!m_xtra)
    {
        m_xtra = new OverflowBuf(pgr);
        m_xtra->allocate();
    }

    if (!optr)
        throw InvalidPageException("Invalid overflow data (null ptr)", pgr.filename());

    //don't read the same overflow page twice for the same record
    if (m_xtra->get_page() != optr)
    {
        pgr.read_page(*m_xtra, optr);
        
        if (!m_xtra->validate())
            throw InvalidPageException("Invalid overflow data", pgr.filename());
    }
}

uint32_t BucketIter::get_hash32()
{
    return m_iter->record().get_hash32();
}

bool BucketIter::next()
{
    //does current bucket have another record in it?
    if (m_iter->iter_next())
        return true;
   
    //no, check next bucket overflow
    if (m_iter->get_bucket_next())
    {
        m_pptr = m_iter->get_page();
        m_iter->pager().read_page(*m_iter, m_iter->get_bucket_next());
        if (!m_iter->validate())
            throw InvalidPageException("Invalid overflow bucket", m_iter->pager().filename());

        m_iter->iter_rewind();
        if (m_iter->iter_next())
            return true;
    }

    //no bucket overflow, or bucket overflow empty, nothing more
    return false;
}

